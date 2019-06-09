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

  public SqlServerAvroFns(Config config, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = config.getInteger("avro.fetchsize", 10000);
    this.codec = CodecFactory.fromString(config.getString("avro.codec", "snappy"));
  }

  @Override
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
          .start();
      avroFile.endTime = DateTime.now().toString();
      avroFile.bytes = new File(avroFile.path).length();

      return avroFile;
    }).toObservable();
  }

  /**
   * {@inheritDoc}
   * <p>Attempts to split table into partitions using the primary key(s).</p>
   * <p>This works best if the table primary keys are a clustered index.</p>
   * <p>If the table cannot be split, a single partition is emitted.</p>
   */
  @Override
  public Observable<AvroFile> getPartitions(Table table, String path, long size) {

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

    // Check if table doesn't meet partitioning criteria
    if (table.bytes == 0 || table.rows == 0 || table.bytes < size || size == 0 ||
        table.columns.stream().noneMatch(c -> c.isPrimaryKey)) {
      String sql = String
          .format(Locale.CANADA, "SELECT %s FROM %s.%s WITH (NOLOCK)", String.join(", ", includedColumns), table.schema,
              table.name);
      return Observable
          .just(new AvroFile(table, sql, finalPath.replace("-%{PART}", ""), includedColumns, excludedColumns));
    } else {

      return Observable.create(emitter -> {

        // How many rows will it take to reach target bytes
        long partitionSize = (size) / (table.bytes / table.rows);

        String primaryKeys = table.columns.stream()
            .filter(c -> c.isPrimaryKey)
            .map(c -> "[" + c.name + "]")
            .collect(Collectors.joining(","));

        String primaryKeys2 = table.columns.stream()
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
              .format("WITH p AS (SELECT %1$s FROM %2$s WITH (NOLOCK) ORDER BY %1$s OFFSET %3$d ROWS FETCH NEXT %4$d ROWS ONLY) "
                      + "SELECT %6$s FROM %2$s AS c WHERE EXISTS (SELECT 1 FROM p WHERE %5$s)",
                  primaryKeys, table.name, offset, partitionSize, primaryKeys2, String.join(", ", includedColumns));

          emitter.onNext(
              new AvroFile(table, sql,
                  finalPath.replace("%{PART}", String.format(Locale.CANADA, "%04d", part++)),
                  includedColumns,
                  excludedColumns));
          offset += partitionSize;
        } while (offset < table.rows);

        emitter.onComplete();

      });
    }
  }

  private boolean isSupportedType(int type) {
    return IntStream.of(supported).anyMatch(x -> x == type);
  }

}