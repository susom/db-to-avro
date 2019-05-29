package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database.Catalog.Schema.Table;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database.Catalog.Schema.Table.Column;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Range;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Observable;
import io.reactivex.exceptions.Exceptions;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerAvroFns implements AvroFns {

  private static final String TABLE_SUFFIX = "_TEMP";
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerAvroFns.class);

  private static int[] supported = {
      Types.BIGINT,
      Types.BINARY,
      Types.BLOB,
      Types.CHAR,
      Types.CLOB,
      Types.DOUBLE,
      Types.INTEGER,
      Types.NCHAR,
      Types.NCLOB,
      Types.NUMERIC,
      Types.NVARCHAR,
      Types.REAL,
      Types.SMALLINT,
      Types.TIMESTAMP,
//      Types.VARBINARY, // This is SpatialLocation in MSSQL
      Types.VARCHAR
  };

  private DatabaseProviderRx.Builder dbb;

  public SqlServerAvroFns(Config config) {
    dbb = DatabaseProviderRx
        .pooledBuilder(config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();
  }

  /**
   * Unlike Oracle, SQL server does not have a ROWID equivalent, so a new column for splitting is needed. In SQL server
   * it's faster to create a new table (with the additional column) than to add a column to an existing table. This also
   * allows creating an index on the new column so the windowing queries are very fast.
   *
   * @param table table to duplicate
   * @return the minumum and maximum values from the new splitting column
   */
  private TempMinMax makeTempTable(final Table table) {
    TempMinMax tmm = new TempMinMax();
    dbb.withConnectionAccess().transact(db -> {
      String altName = table.name + TABLE_SUFFIX;
      String catalog = table.getSchema().getCatalog().name;
      String schema = table.getSchema().name;

      db.get().underlyingConnection().setCatalog(catalog);

      db.get().ddl(String.format(Locale.CANADA,
          // For the splitting column, a random 8-bit positive integer is created. This is much faster than using
          // an identity column as SQL server will copy the table in parallel, identity will force a single thread.
          // Note this does mean the order of rows from the original table is lost during the export.
          "SELECT *, ABS(CAST(CAST(NEWID() AS BINARY(8)) AS INT)) AS xSPLIT_IDx INTO %1$s.%2$s_TEMP FROM %1$s.%2$s",
          schema, table.name)).execute();

      db.get().ddl(String.format(Locale.CANADA,
          "CREATE CLUSTERED INDEX IX_%2$s_SPLIT_ID ON %1$s.%2$s (xSPLIT_IDx)", schema, altName))
          .execute();

      tmm.min = db.get()
          .toSelect(
              String.format(Locale.CANADA, "SELECT MIN(xSPLIT_IDx) FROM %s.%s", schema, altName))
          .queryLongOrZero();

      tmm.max = db.get()
          .toSelect(
              String.format(Locale.CANADA, "SELECT MAX(xSPLIT_IDx) FROM %s.%s", schema, altName))
          .queryLongOrZero();

      table.tempName = altName;
    });
    return tmm;
  }

  @Override
  public Observable<Range> getRanges(final Table table, long divisions) {
    return Observable.create(emitter -> {
      try {
        TempMinMax tmm = makeTempTable(table);
        int index = 0;
        long windowSize = Math.floorDiv(tmm.max - tmm.min, divisions) + 1;
        long currentRow = tmm.min;

        do {
          long next = currentRow + windowSize;
          if (next < tmm.max) {
            emitter.onNext(new Range(index++, currentRow, currentRow + windowSize));
          } else {
            emitter.onNext(new Range(index++, currentRow, tmm.max, true));
          }
          currentRow += windowSize;
        } while (currentRow < tmm.max);

        emitter.onComplete();

      } catch (Exception ex) {
        Exceptions.propagate(ex);
      }
    });
  }

  @Override
  public void cleanup(final Table table) {
    if (table.tempName == null) {
      return;
    }
    dbb.withConnectionAccess().transact(db -> {
      db.get().underlyingConnection().setCatalog(table.getSchema().getCatalog().name);
      db.get().ddl(String.format(Locale.CANADA, "DROP TABLE %1$s.%2$s", table.getSchema().name, table.tempName))
          .execute();
    });
  }

  @Override
  public AvroFile saveAsAvro(Table table, Range range, String pathExpr) {
    AvroFile avroFile = new AvroFile(table);
    dbb.withConnectionAccess().transact(db -> {
      avroFile.startTime = DateTime.now().toString();

      String catalog = avroFile.table.getSchema().getCatalog().name;
      String schema = avroFile.table.getSchema().name;
      String tempName = avroFile.table.tempName;

      db.get().underlyingConnection().setCatalog(catalog);

      // Only dump the supported column types
      List<String> columns = new ArrayList<>();
      for (Column column : avroFile.table.columns) {
        if (isSupportedType(column.type)) {
          columns.add(column.name);
          avroFile.includedRows.add(column.name);
        } else {
          avroFile.omittedRows.add(column.name);
        }
      }

      String path = pathExpr.replace("%{TABLE}", avroFile.table.name)
          .replace("%{SCHEMA}", avroFile.table.getSchema().name)
          .replace("%{PART}", String.format("%03d", range.index))
          .replace("%{START}", String.valueOf(range.start))
          .replace("%{END}", String.valueOf(range.end));

      Etl.saveQuery(db.get().toSelect(String
          .format(Locale.CANADA, "SELECT %s FROM %s.%s WHERE xSPLIT_IDx >= %d AND xSPLIT_IDx %s %d",
              String.join(", ", columns), avroFile.table.getSchema().name, tempName, range.start,
              range.terminal ? "<=" : "<",
              range.end))).asAvro(path, schema, avroFile.table.name).withCodec(CodecFactory.snappyCodec())
          .fetchSize(5000)
          .start();

      avroFile.endTime = DateTime.now().toString();
      avroFile.path = path;
    });
    return avroFile;
  }

  @Override
  public AvroFile saveAsAvro(Table table, String pathExpr) {
    AvroFile avroFile = new AvroFile(table);
    dbb.withConnectionAccess().transact(db -> {
      String catalog = table.getSchema().getCatalog().name;
      String schema = table.getSchema().name;

      avroFile.startTime = DateTime.now().toString();

      db.get().underlyingConnection().setCatalog(catalog);

      // Only dump the supported column types
      List<String> columns = new ArrayList<>();
      for (Column column : table.columns) {
        if (isSupportedType(column.type)) {
          columns.add(column.name);
          avroFile.includedRows.add(column.name);
        } else {
          avroFile.omittedRows.add(column.name);
        }
      }

      String path = pathExpr.replace("%{TABLE}", avroFile.table.name)
          .replace("%{SCHEMA}", avroFile.table.getSchema().name)
          .replace("%{PART}", "000")
          .replace("%{START}", "0")
          .replace("%{END}", String.valueOf(table.rows));

      Etl.saveQuery(
          db.get().toSelect(
              String.format(Locale.CANADA, "SELECT %s FROM %s.%s", String.join(", ", columns), schema,
                  table.name)))
          .asAvro(path, schema, table.name).withCodec(CodecFactory.snappyCodec()).fetchSize(5000).start();

      avroFile.endTime = DateTime.now().toString();
      avroFile.path = path;
    });
    return avroFile;
  }

  private boolean isSupportedType(int type) {
    return IntStream.of(supported).anyMatch(x -> x == type);
  }

  class TempMinMax {

    long min;
    long max;
  }

}