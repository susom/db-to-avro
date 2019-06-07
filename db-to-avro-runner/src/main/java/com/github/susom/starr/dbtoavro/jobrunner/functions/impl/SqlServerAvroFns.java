package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.database.DatabaseProviderVertx;
import com.github.susom.database.Sql;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.BoundedRange;
import com.github.susom.starr.dbtoavro.jobrunner.entity.BoundedRange.SqlObject;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Column;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class SqlServerAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerAvroFns.class);

  private static final int FETCH_SIZE = 5000;

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

  private final DatabaseProviderRx.Builder dbb;
  private final Config config;

  public SqlServerAvroFns(Config config, DatabaseProviderRx.Builder dbb) {
    this.config = config;
    this.dbb = dbb;
  }

  public Maybe<Column> getSplitterColumn(Table table, long threshold) {
    return Observable.fromIterable(table.columns)
        .filter(check -> table.bytes > threshold)
        .filter(column -> column.isPrimaryKey)
        .map(column -> {
              dbb.withConnectionAccess().transactRx(db -> {
                LOGGER.debug("Comparing distinct column count for {} {}", table.name, column.name);
                db.get().underlyingConnection().setCatalog(table.catalog);
                db.get().underlyingConnection().setSchema(table.schema);
                Sql sql = new Sql(String.format("SELECT COUNT(DISTINCT %s) FROM %s", column.name, table.name));
                column.distinct = db.get().toSelect(sql).queryLongOrZero();
              });
              return column;
            }
        ).reduce((column, column2) -> column.distinct > column2.distinct ? column : column2);
  }

  public Observable<BoundedRange> getTableRanges(Table table, Column column, long divisions) {
    LOGGER.debug("Splitting {} in to {} parts", table.name, divisions);
    return dbb.withConnectionAccess().transactRx(db -> {
      db.get().underlyingConnection().setCatalog(table.catalog);
      db.get().underlyingConnection().setSchema(table.schema);

      List<BoundedRange> ranges = new ArrayList<>();

      Connection con = db.get().underlyingConnection();
      String sql = String.format(
          "SELECT %1$s\n"
              + "FROM (\n"
              + "         SELECT %1$s, ROW_NUMBER() OVER (ORDER BY %1$s) AS ROWNUM\n"
              + "         FROM %2$s WITH (NOLOCK)\n"
              + "     ) \n"
              + "  AS SEGMENTS \n"
              + "WHERE (SEGMENTS.ROWNUM %% %3$s = 0)\n"
              + "   OR (SEGMENTS.ROWNUM = 1)\n"
              + "   OR (SEGMENTS.ROWNUM = %4$s)",
          column.name, table.name, (table.rows / divisions), table.rows);

      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          SqlObject previous = null;
          int row = 0;
          while (rs.next()) {
            SqlObject current;
            current = new SqlObject(rs.getMetaData().getColumnName(1), rs.getObject(1));
            if (previous != null) {
              BoundedRange range = new BoundedRange(previous, current, row++);
              if (range.index == divisions) {
                range.terminal = true;
              }
              ranges.add(range);
            }
            previous = current;
          }
        }
      }

      return ranges;
    }).toObservable().flatMapIterable(l -> l);
  }

  @Override
  public Single<AvroFile> saveAsAvro(Table table, BoundedRange bounds, String pathExpr) {
    return dbb.withConnectionAccess().transactRx(db -> {
      AvroFile avroFile = new AvroFile(table);
      avroFile.startTime = DateTime.now().toString();

      String catalog = table.catalog;
      String schema = table.schema;
      String tableName = table.name;

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
              String.format(Locale.CANADA, "SELECT %s FROM %s.%s WITH (NOLOCK) \n",
                  String.join(", ", columns), schema, tableName))
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
      } else if (bounds.lower.getValue() instanceof java.sql.Timestamp) {
        sql.argDate((Timestamp) bounds.lower.getValue());
      } else if (bounds.lower.getValue() instanceof java.sql.Date) {
        sql.argDate((Date) bounds.lower.getValue());
      } else {
        LOGGER.error("Didn't know how to cast class of type {} in lower bounds", bounds.lower.getValue().getClass());
      }
      sql.append(") AND (");

      // Upper bounds
      sql.append(String.format("%s %s ?", bounds.upper.getName(), (bounds.terminal) ? "<=" : "<"));
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
      } else if (bounds.lower.getValue() instanceof java.sql.Timestamp) {
        sql.argDate((Timestamp) bounds.lower.getValue());
      } else if (bounds.lower.getValue() instanceof java.sql.Date) {
        sql.argDate((Date) bounds.lower.getValue());
      } else {
        LOGGER.error("Didn't know how to cast class of type {} in lower bounds", bounds.upper.getValue().getClass());
      }
      sql.append(")");

      Etl.saveQuery(db.get().toSelect(sql)).asAvro(path, schema, avroFile.table.name)
          .withCodec(CodecFactory.snappyCodec())
          .fetchSize(FETCH_SIZE)
          .start();

      avroFile.endTime = DateTime.now().toString();
      avroFile.path = path;
      return avroFile;
    }).toSingle();
  }

  @Override
  public Single<AvroFile> saveAsAvro(Table table, String pathExpr) {
    return dbb.withConnectionAccess().transactRx(db -> {
      AvroFile avroFile = new AvroFile(table);
      String catalog = table.catalog;
      String schema = table.schema;

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
          .replace("-%{PART}", "")
          .replace("%{PART}-", "")
          .replace("%{START}", "0")
          .replace("%{END}", String.valueOf(table.rows));

      LOGGER.info("Writing {}", path);

      Etl.saveQuery(
          db.get().toSelect(
              String.format(Locale.CANADA, "SELECT %s FROM %s.%s WITH (NOLOCK)", String.join(", ", columns), schema,
                  table.name)))
          .asAvro(path, schema, table.name)
          .withCodec(CodecFactory.snappyCodec())
          .fetchSize(FETCH_SIZE)
          .start();

      avroFile.endTime = DateTime.now().toString();
      avroFile.path = path;
      return avroFile;
    }).toSingle();
  }

  private boolean isSupportedType(int type) {
    return IntStream.of(supported).anyMatch(x -> x == type);
  }

}