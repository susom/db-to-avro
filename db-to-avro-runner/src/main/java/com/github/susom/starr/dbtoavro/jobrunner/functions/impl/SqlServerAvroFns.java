package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.database.Sql;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.BoundedRange;
import com.github.susom.starr.dbtoavro.jobrunner.entity.BoundedRange.SqlObject;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database.Catalog.Schema.Table;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database.Catalog.Schema.Table.Column;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.IntStream;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class SqlServerAvroFns implements AvroFns {

  private static final String TEMP_SUFFIX = "_XSPLITX";
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
      String altName = table.name + TEMP_SUFFIX;
      String catalog = table.getSchema().getCatalog().name;
      String schema = table.getSchema().name;

      db.get().underlyingConnection().setCatalog(catalog);

      db.get().ddl(String.format(Locale.CANADA, "DROP TABLE IF EXISTS %s.%s", schema, altName)).execute();

      db.get().ddl(String.format(Locale.CANADA,
          // For the splitting column, a random 8-bit positive integer is created. This is much faster than using
          // an identity column as SQL server will copy the table in parallel, identity will force a single thread.
          // Note this does mean the order of rows from the original table is lost during the export.
          "SELECT *, ABS(CAST(CAST(NEWID() AS BINARY(8)) AS INT)) AS XSPLIT_IDX INTO %1$s.%2$s%3$s FROM %1$s.%2$s",
          schema, table.name, TEMP_SUFFIX)).execute();

      db.get().ddl(String.format(Locale.CANADA,
          "CREATE CLUSTERED INDEX IX_SPLIT_ID ON %s.%s (XSPLIT_IDX)", schema, altName))
          .execute();

      tmm.min = db.get()
          .toSelect(
              String.format(Locale.CANADA, "SELECT MIN(XSPLIT_IDX) FROM %s.%s", schema, altName))
          .queryLongOrZero();

      tmm.max = db.get()
          .toSelect(
              String.format(Locale.CANADA, "SELECT MAX(XSPLIT_IDX) FROM %s.%s", schema, altName))
          .queryLongOrZero();

      table.tempName = altName;
      LOGGER.debug("Done splitting table {}", table.name);

    });
    return tmm;
  }

  private Optional<Column> getSplitterColumn(Table table) {
    if (table.columns.stream().noneMatch(c -> c.pk)) {
      return Optional.empty();
    }

    Map<Column, Long> counts = new HashMap<>();
    dbb.withConnectionAccess().transact(dbb -> {
      dbb.get().underlyingConnection().setCatalog(table.getSchema().getCatalog().name);
      table.columns.forEach(col -> {
        if (!col.pk) {
          return;
        }
        Sql sql = new Sql(String.format("SELECT COUNT(DISTINCT %s) FROM %s", col.name, table.name));
        counts.put(col, dbb.get().toSelect(sql).queryLongOrZero());
      });
    });

    Column col = null;
    long max = 0;
    for (Entry<Column, Long> entry : counts.entrySet()) {
      LOGGER.debug("Comparing {}: {}", entry.getKey().name, entry.getValue());
      if (entry.getValue() > max) {
        max = entry.getValue();
        col = entry.getKey();
      }
    }

    if (col != null) {
      LOGGER.debug("{} is the best column for splitting", col.name);
      return Optional.of(col);
    }

    return Optional.empty();

  }

  public Observable<BoundedRange> getTableRanges(Table table, long divisions) {
    LOGGER.debug("Splitting {} in to {} parts", table.name, divisions);
    return dbb.withConnectionAccess().transactRx(db -> {
      db.get().underlyingConnection().setCatalog(table.getSchema().getCatalog().name);

      Optional<Column> getSplit = getSplitterColumn(table);
      Column splitColumn;
      if (!getSplit.isPresent()) {
        LOGGER.error("No column is good for splitting!!");
        return null;
      } else {
        splitColumn = getSplit.get();
      }

      Connection con = db.get().underlyingConnection();
      String sql = String.format(
          "SELECT %1$s\n"
              + "FROM (\n"
              + "         SELECT %1$s, ROW_NUMBER() OVER (ORDER BY %1$s) AS ROWNUM\n"
              + "         FROM %2$s\n"
              + "     )\n"
              + "  AS SEGMENTS\n"
              + "WHERE (SEGMENTS.ROWNUM %% %3$s = 0)\n"
              + "   OR (SEGMENTS.ROWNUM = 1)\n"
              + "   OR (SEGMENTS.ROWNUM = %4$s)",
          splitColumn.name, table.name, (table.rows / divisions), table.rows);

      Statement stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(sql);

      List<BoundedRange> bounds = new ArrayList<>();
      SqlObject previous = null;
      int row = 0;
      while (rs.next()) {
        SqlObject current;
        current = new SqlObject(rs.getMetaData().getColumnName(1), rs.getObject(1));
        if (previous != null) {
          bounds.add(new BoundedRange(previous, current, row++));
        }
        previous = current;
      }

      bounds.get(bounds.size() - 1).terminal = true;

      return bounds;

    }).toSingle().flattenAsObservable(l -> l);

  }


  @Override
  public Single<AvroFile> saveAsAvro(Table table, BoundedRange bounds, String pathExpr) {
    return dbb.withConnectionAccess().transactRx(db -> {
      AvroFile avroFile = new AvroFile(table);
      avroFile.startTime = DateTime.now().toString();

      String catalog = avroFile.table.getSchema().getCatalog().name;
      String schema = avroFile.table.getSchema().name;
      String tableName = avroFile.table.name;

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
          .replace("%{SCHEMA}", schema)
          .replace("%{CATALOG}", catalog)
          .replace("%{PART}", String.format("%03d", bounds.index));

      LOGGER.info("Writing {}", path);

      Sql sql = new Sql()
          .append(
              String.format(Locale.CANADA, "SELECT %s FROM %s.%s \n", String.join(", ", columns), schema, tableName))
          .append("WHERE (");

      sql.append(String.format("%s >= ?", bounds.lower.getName()));
      if (bounds.lower.getValue() instanceof java.lang.String) {
        sql.argString((String) bounds.lower.getValue());
      } else if (bounds.lower.getValue() instanceof java.lang.Long) {
        sql.argLong((Long) bounds.lower.getValue());
      } else if (bounds.lower.getValue() instanceof java.lang.Integer) {
        sql.argInteger((Integer) bounds.lower.getValue());
      } else if (bounds.lower.getValue() instanceof java.lang.Short) {
        sql.argInteger(Integer.valueOf((Short) bounds.lower.getValue()));
      } else if (bounds.lower.getValue() instanceof java.math.BigDecimal) {
        sql.argBigDecimal((BigDecimal) bounds.lower.getValue());
      } else {
        LOGGER.error("Didn't know how to cast class of type {} in lower bounds", bounds.lower.getValue().getClass());
      }
      sql.append(") AND (");

      // Upper bounds
      sql.append(String.format("%s %s ?", bounds.upper.getName(), bounds.terminal ? "<=" : "<"));
      if (bounds.upper.getValue() instanceof java.lang.String) {
        sql.argString((String) bounds.upper.getValue());
      } else if (bounds.upper.getValue() instanceof java.lang.Long) {
        sql.argLong((Long) bounds.upper.getValue());
      } else if (bounds.upper.getValue() instanceof java.lang.Integer) {
        sql.argInteger((Integer) bounds.upper.getValue());
      } else if (bounds.upper.getValue() instanceof java.lang.Short) {
        sql.argInteger(Integer.valueOf((Short) bounds.upper.getValue()));
      } else if (bounds.upper.getValue() instanceof java.math.BigDecimal) {
        sql.argBigDecimal((BigDecimal) bounds.upper.getValue());
      } else {
        LOGGER.error("Didn't know how to cast class of type {} in lower bounds", bounds.upper.getValue().getClass());
      }
      sql.append(")");

      Etl.saveQuery(db.get().toSelect(sql)).asAvro(path, schema, avroFile.table.name)
          .withCodec(CodecFactory.snappyCodec())
          .fetchSize(5000)
          .start();

      avroFile.endTime = DateTime.now().toString();
      avroFile.path = path;
      return avroFile;
    }).toSingle();
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
  public Single<AvroFile> saveAsAvro(Table table, String pathExpr) {
    return dbb.withConnectionAccess().transactRx(db -> {
      AvroFile avroFile = new AvroFile(table);
      String catalog = table.getSchema().getCatalog().name;
      String schema = table.getSchema().name;

      avroFile.startTime = DateTime.now().toString();

      db.get().underlyingConnection().setCatalog(catalog);

      // Only dump the supported column types
      List<String> columns = new ArrayList<>();
      for (Column column : table.columns) {
        if (isSupportedType(column.type)) {
          columns.add("[" + column.name + "]");
          avroFile.includedRows.add(column.name);
        } else {
          avroFile.omittedRows.add(column.name);
        }
      }

      String path = pathExpr.replace("%{TABLE}", avroFile.table.name)
          .replace("%{SCHEMA}", schema)
          .replace("%{CATALOG}", catalog)
//          .replace("-%{PART}", "")
//          .replace("%{PART}-", "")
          .replace("%{START}", "0")
          .replace("%{END}", String.valueOf(table.rows));

      LOGGER.info("Writing {}", path);

      if (table.bytes > 1000000000) {
        Etl.saveQuery(
            db.get().toSelect(
                String.format(Locale.CANADA, "SELECT %s FROM %s.%s", String.join(", ", columns), schema,
                    table.name)))
            .asAvro(path, schema, table.name).withCodec(CodecFactory.snappyCodec()).fetchSize(5000).rowsPerFile(1000000)
            .start();
      } else {
        Etl.saveQuery(
            db.get().toSelect(
                String.format(Locale.CANADA, "SELECT %s FROM %s.%s", String.join(", ", columns), schema,
                    table.name)))
            .asAvro(path, schema, table.name).withCodec(CodecFactory.snappyCodec()).fetchSize(5000).start();

      }

      avroFile.endTime = DateTime.now().toString();
      avroFile.path = path;
      return avroFile;
    }).toSingle();
  }

  private boolean isSupportedType(int type) {
    return IntStream.of(supported).anyMatch(x -> x == type);
  }

  class TempMinMax {

    long min;
    long max;
  }

}