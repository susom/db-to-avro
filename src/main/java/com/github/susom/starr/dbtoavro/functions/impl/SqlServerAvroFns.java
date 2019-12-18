package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Single;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.file.CodecFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerAvroFns.class);
  private static int STRING_DATE_CONVERSION = 126;

  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean tidyTables;
  private boolean stringDate;
  private String stringDateSuffix;

  public SqlServerAvroFns(Job job, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = job.fetchRows;
    this.codec = CodecFactory.fromString(job.codec);
    this.tidyTables = job.tidyTables;
    this.stringDate = job.stringDate;
    this.stringDateSuffix = job.stringDateSuffix;
  }

  @Override
  public Single<AvroFile> saveAsAvro(final Query query) {
    return dbb.transactRx(db -> {

      String startTime = DateTime.now().toString();
      db.get().underlyingConnection().setCatalog(query.table.catalog);

      Etl.SaveAsAvro avro = Etl.saveQuery(db.get().toSelect(query.sql))
        .asAvro(query.path, query.table.schema, query.table.name)
        .withCodec(codec)
        .fetchSize(fetchSize);

      if (tidyTables) {
        avro = avro.tidyNames();
      }

      List<String> paths = new ArrayList<>();
      long rows = 0;
      if (query.batchSize > 0) {
        LOGGER.info("Writing {}", query.path);
        Map<String, Long> output = avro.start(query.batchSize);
        for (Map.Entry<String, Long> entry : output.entrySet()) {
          paths.add(entry.getKey());
          rows += entry.getValue();
        }
      } else {
        LOGGER.info("Writing {}", query.path);
        rows = avro.start();
        paths.add(query.path);
      }

      String endTime = DateTime.now().toString();

      return new AvroFile(query, paths, startTime, endTime, new File(query.path).length(), rows);

    }).toSingle();
  }

  @Override
  public Single<Query> query(final Table table, final long targetSize, final String pathPattern) {
    return Single.create(emitter -> {
        // Only dump the supported column types
        String columns = getColumnSql(table);

        String sql = String
          .format(Locale.ROOT, "SELECT %s FROM [%s].[%s] WITH (NOLOCK)", columns, table.schema,
            table.name);

        String path;

        long rowsPerFile = 0;
        if (targetSize > 0 && table.bytes > 0 && table.rows > 0 && table.bytes > targetSize) {
          path = pathPattern
            .replace("%{CATALOG}", table.catalog == null ? "catalog" : tidy(table.catalog))
            .replace("%{SCHEMA}", table.schema == null ? "schema" : tidy(table.schema))
            .replace("%{TABLE}", tidy(table.name));
          rowsPerFile = (targetSize) / (table.bytes / table.rows);
        } else {
          path = pathPattern
            .replace("%{CATALOG}", table.catalog == null ? "catalog" : tidy(table.catalog))
            .replace("%{SCHEMA}", table.schema == null ? "schema" : tidy(table.schema))
            .replace("%{TABLE}", tidy(table.name))
            .replace("-%{PART}", "");

        }

        emitter.onSuccess(new Query(table, sql, rowsPerFile, path));

      }
    );
  }

  private String getColumnSql(Table table) {
    return table.columns.stream()
      .filter(col -> col.serializable)
      .map(col -> {
        // Use column name string not JDBC type val to avoid mappings
        if (stringDate && col.typeName.equals("datetime")) {
          return String.format(Locale.ROOT, "CONVERT(varchar, [%s], %d) AS [%s%s]",
            col.name,
            STRING_DATE_CONVERSION,
            col.name,
            stringDateSuffix);
        } else {
          return "[" + col.name + "]";
        }
      })
      .collect(Collectors.joining(", "));
  }

  private String tidy(final String name) {
    if (name != null && tidyTables) {
      return name
        .replaceAll("[^a-zA-Z0-9]", " ")
        .replaceAll("\\s", "_")
        .trim()
        .toLowerCase(Locale.ROOT);
    } else {
      return name;
    }
  }

}
