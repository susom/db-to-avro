package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Query;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Observable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerAvroFns.class);

  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean optimized;
  private boolean tidy;

  public SqlServerAvroFns(Config config, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = config.getInteger("avro.fetchsize", 10000);
    this.codec = CodecFactory.fromString(config.getString("avro.codec", "snappy"));
    this.optimized = config.getBooleanOrFalse("sqlserver.optimized.enable");
    this.tidy = config.getBooleanOrFalse("avro.tidy");
  }

  @Override
  public Observable<AvroFile> saveAsAvro(final Query query) {
    return dbb.withConnectionAccess().transactRx(db -> {

      String startTime = DateTime.now().toString();
      db.get().underlyingConnection().setCatalog(query.table.catalog);

      List<String> paths = new ArrayList<>();
      if (query.rowsPerFile > 0) {
        LOGGER.info("Writing {}", query.path);
        paths.addAll(Etl.saveQuery(
            db.get().toSelect(query.sql))
            .asAvro(query.path, query.table.schema, query.table.name)
            .withCodec(CodecFactory.snappyCodec())
            .withCodec(codec)
            .fetchSize(fetchSize)
            .withTidy(tidy)
            .start(query.rowsPerFile));
      } else {
        LOGGER.info("Writing {}", query.path);
        Etl.saveQuery(
            db.get().toSelect(query.sql))
            .asAvro(query.path, query.table.schema, query.table.name)
            .withCodec(CodecFactory.snappyCodec())
            .withCodec(codec)
            .fetchSize(fetchSize)
            .withTidy(tidy)
            .start();
        paths.add(query.path);
      }

      String endTime = DateTime.now().toString();

      return new AvroFile(query, paths, startTime, endTime, new File(query.path).length());

    }).toObservable();
  }

  @Override
  public Observable<Query> query(final Table table, final long targetSize, final String pathPattern) {

    // Only dump the supported column types
    String columns = table.columns.stream()
        .filter(c -> c.serializable)
        .map(c -> "[" + c.name + "]")
        .collect(Collectors.joining(", "));

    String sql = String
        .format(Locale.CANADA, "SELECT %s FROM %s.%s WITH (NOLOCK)", columns, table.schema,
            table.name);

    String path;

    long rowsPerFile = 0;
    if (targetSize > 0 && table.bytes > 0 && table.rows > 0 && table.bytes > targetSize) {
      path = pathPattern
          .replace("%{CATALOG}", tidy(table.catalog))
          .replace("%{SCHEMA}", tidy(table.schema))
          .replace("%{TABLE}", tidy(table.name));
      rowsPerFile = (targetSize) / (table.bytes / table.rows);
    } else {
      path = pathPattern
          .replace("%{CATALOG}", tidy(table.catalog))
          .replace("%{SCHEMA}", tidy(table.schema))
          .replace("%{TABLE}", tidy(table.name))
          .replace("-%{PART}", "");

    }

    return Observable.just(new Query(table, sql, rowsPerFile, path));

  }


  /**
   * {@inheritDoc}
   * <p>Attempts to split table into partitions using the primary key(s).</p>
   * <p>This works best if the table primary keys are a clustered index.</p>
   * <p>If the table cannot be split, a single partition is emitted.</p>
   */
  @Override
  public Observable<Query> optimizedQuery(final Table table, final long targetSize, final String pathPattern) {

    // Check if table doesn't meet partitioning criteria, if not, bail.
    if (!optimized
        || table.bytes == 0
        || table.rows == 0
        || table.bytes < targetSize
        || targetSize == 0
        || table.columns.stream().noneMatch(c -> c.primaryKey)
    ) {
      return Observable.empty();
    }

    // Otherwise split the table using a naive primary key splitting method
    return Observable.create(emitter -> {

      // Only dump the supported column types
      String columns = table.columns.stream()
          .filter(c -> c.serializable)
          .map(c -> "[" + c.name + "]")
          .collect(Collectors.joining(", "));

      // Estimate how many rows it will take to reach the target file size for avro output
      long partitionSize = (targetSize) / (table.bytes / table.rows);

      String primaryKeys = table.columns.stream()
          .filter(c -> c.primaryKey)
          .map(c -> "[" + c.name + "]")
          .collect(Collectors.joining(","));

      String joinKeys = table.columns.stream()
          .filter(c -> c.primaryKey)
          .map(c -> "p.[" + c.name + "] = c.[" + c.name + "]")
          .collect(Collectors.joining(" AND "));

      long offset = 0;
      int part = 0;
      do {
        if (offset + partitionSize > table.rows) {
          partitionSize = (table.rows - offset);
        }
        String sql = String
            .format(Locale.CANADA,
                "WITH p AS (SELECT %1$s FROM %2$s WITH (NOLOCK) ORDER BY %1$s OFFSET %3$d ROWS FETCH NEXT %4$d ROWS ONLY) "
                    + "SELECT %6$s FROM %2$s AS c WHERE EXISTS (SELECT 1 FROM p WHERE %5$s)",
                primaryKeys, table.name, offset, partitionSize, joinKeys, columns);

        String path = pathPattern
            .replace("%{CATALOG}", tidy(table.catalog))
            .replace("%{SCHEMA}", tidy(table.schema))
            .replace("%{TABLE}", tidy(table.name))
            .replace("%{PART}", String.format(Locale.CANADA, "%03d", part++));

        emitter.onNext(new Query(table, sql, 0, path));

        offset += partitionSize;
      } while (offset < table.rows);

      emitter.onComplete();

    });

  }

  private String tidy(final String name) {
    if (tidy) {
      return name
          .replaceAll("[^a-zA-Z0-9]", " ")
          .replaceAll("\\s", "_")
          .trim()
          .toLowerCase(Locale.CANADA);
    } else {
      return name;
    }
  }

}