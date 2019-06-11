package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Column;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Observable;
import java.io.File;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerAvroFns implements AvroFns {

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
      Types.TINYINT,
      Types.TIMESTAMP,
//      Types.VARBINARY, // This is SpatialLocation in MSSQL
      Types.VARCHAR
  };

  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean dbSplit;

  public SqlServerAvroFns(Config config, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = config.getInteger("avro.fetchsize", 10000);
    this.codec = CodecFactory.fromString(config.getString("avro.codec", "snappy"));
    this.dbSplit = config.getBooleanOrFalse("sqlserver.dbsplit");
  }

  @Override
  // TODO: This needs to emit multiple objects if the ETL has split the file
  public Observable<AvroFile> saveAvroFile(final AvroFile avroFile) {
    return dbb.withConnectionAccess().transactRx(db -> {
      Table table = avroFile.table;
      LOGGER.info("Writing {}", avroFile.path);

      avroFile.startTime = DateTime.now().toString();
      db.get().underlyingConnection().setCatalog(table.catalog);
      Etl.saveQuery(
          db.get().toSelect(avroFile.sql))
          .asAvro(avroFile.path, table.schema, table.name)
          .withCodec(CodecFactory.snappyCodec())
          .withCodec(codec)
          .fetchSize(fetchSize)
          .rowsPerFile(avroFile.rowsPerFile)
          .start();

      avroFile.endTime = DateTime.now().toString();
      avroFile.bytes = new File(avroFile.path).length();

      return avroFile;
    }).toObservable();
  }

  @Override
  public Observable<AvroFile> singleQuery(final Table table, final String path, final long targetSize) {
    // Only dump the supported column types
    List<String> includedColumns = new ArrayList<>();
    List<String> excludedColumns = new ArrayList<>();
    for (Column column : table.columns) {
      if (isSupportedType(column.type)) {
        includedColumns.add("[" + column.name + "]");
      } else {
        excludedColumns.add(column.name);
      }
    }

    String finalPath = path.replace("%{TABLE}", table.name)
        .replace("%{SCHEMA}", table.schema)
        .replace("%{CATALOG}", table.catalog);

    String sql = String
        .format(Locale.CANADA, "SELECT %s FROM %s.%s WITH (NOLOCK)", String.join(", ", includedColumns), table.schema,
            table.name);

    long rowsPerFile = 0;
    if (targetSize > 0 && table.bytes > 0 && table.rows > 0) {
      rowsPerFile = (targetSize) / (table.bytes / table.rows);
    }

    return Observable.just(new AvroFile(table, sql, finalPath, includedColumns, excludedColumns, rowsPerFile));
  }


  /**
   * {@inheritDoc}
   * <p>Attempts to split table into partitions using the primary key(s).</p>
   * <p>This works best if the table primary keys are a clustered index.</p>
   * <p>If the table cannot be split, a single partition is emitted.</p>
   */
  @Override
  public Observable<AvroFile> multipleQuery(final Table table, final String path, final long targetSize) {

    // Check if table doesn't meet partitioning criteria, if not, bail.
    if (!dbSplit || table.bytes == 0 || table.rows == 0 || table.bytes < targetSize || targetSize == 0 ||
        table.columns.stream().noneMatch(c -> c.isPrimaryKey)) {
      return Observable.empty();
    }

    // Otherwise split the table using a naive primary key splitting method
    return Observable.create(emitter -> {

      // Only dump the supported column types
      List<String> includedColumns = new ArrayList<>();
      List<String> excludedColumns = new ArrayList<>();
      for (Column column : table.columns) {
        if (isSupportedType(column.type)) {
          includedColumns.add("[" + column.name + "]");
        } else {
          excludedColumns.add(column.name);
        }
      }

      String finalPath = path.replace("%{TABLE}", table.name)
          .replace("%{SCHEMA}", table.schema)
          .replace("%{CATALOG}", table.catalog);

      // Estimate how many rows it will take to reach the target file size for avro output
      long partitionSize = (targetSize) / (table.bytes / table.rows);

      String primaryKeys = table.columns.stream()
          .filter(c -> c.isPrimaryKey)
          .map(c -> "[" + c.name + "]")
          .collect(Collectors.joining(","));

      String joinKeys = table.columns.stream()
          .filter(c -> c.isPrimaryKey)
          .map(c -> "p.[" + c.name + "] = c.[" + c.name + "]")
          .collect(Collectors.joining(" AND "));

      long offset = 0;
      int part = 0;
      do {
        if (offset + partitionSize > table.rows) {
          partitionSize = (table.rows - offset);
        }
        String sql = String
            .format(
                "WITH p AS (SELECT %1$s FROM %2$s WITH (NOLOCK) ORDER BY %1$s OFFSET %3$d ROWS FETCH NEXT %4$d ROWS ONLY) "
                    + "SELECT %6$s FROM %2$s AS c WHERE EXISTS (SELECT 1 FROM p WHERE %5$s)",
                primaryKeys, table.name, offset, partitionSize, joinKeys, String.join(", ", includedColumns));

        emitter.onNext(
            new AvroFile(table, sql, finalPath.replace("%{PART}", String.format(Locale.CANADA, "%04d", part++)),
                includedColumns, excludedColumns));

        offset += partitionSize;
      } while (offset < table.rows);

      emitter.onComplete();

    });

  }

  private boolean isSupportedType(int type) {
    return IntStream.of(supported).anyMatch(x -> x == type);
  }

}