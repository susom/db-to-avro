package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Observable;
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

public class OracleAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleAvroFns.class);

  private static final String STRING_DATE_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS";
  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean tidyTables;
  private boolean stringDate;
  private String stringDateSuffix;

  public OracleAvroFns(Job job, DatabaseProviderRx.Builder dbb) {
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

      db.get().ddl("ALTER SESSION DISABLE PARALLEL QUERY").execute();
      db.get().ddl("ALTER SESSION SET \"_SERIAL_DIRECT_READ\" = TRUE").execute();

      String startTime = DateTime.now().toString();

      Etl.SaveAsAvro avro = Etl.saveQuery(db.get().toSelect(query.sql))
        .asAvro(query.path, query.table.schema, query.table.name)
        .withCodec(CodecFactory.snappyCodec())
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
      LOGGER.info("Writing {} has finished", query.path);

      return new AvroFile(query, paths, startTime, endTime, new File(query.path).length(), rows);

    }).toSingle();
  }

  @Override
  public Single<Query> query(final Table table, final long targetSize, final String pathPattern) {
    return Single.create(emitter -> {
      // Only dump the supported column types
      String columns = getColumnSql(table);

      String sql = String
        .format(Locale.ROOT, "SELECT %s FROM \"%s\".\"%s\"", columns, table.schema,
          table.name);

      String path;

      long rowsPerFile = 0;
      if (targetSize > 0 && table.bytes > 0 && table.rows > 0 && table.bytes > targetSize) {
        path = pathPattern
          .replace("%{CATALOG}", "ANY")
          .replace("%{SCHEMA}", tidy(table.schema))
          .replace("%{TABLE}", tidy(table.name));
        rowsPerFile = (targetSize) / (table.bytes / table.rows);
      } else {
        path = pathPattern
          .replace("%{CATALOG}", "ANY")
          .replace("%{SCHEMA}", tidy(table.schema))
          .replace("%{TABLE}", tidy(table.name))
          .replace("-%{PART}", "");

      }

      emitter.onSuccess(new Query(table, sql, rowsPerFile, path));

    });
  }

  private String getColumnSql(Table table) {
    return table.columns.stream()
      .filter(c -> c.serializable)
      .map(c -> {
        // Use column name string (DATE) not java.sql.Type since JDBC is TIMESTAMP
        if (stringDate && c.typeName.equals("DATE")) {
          return String.format(Locale.ROOT, "TO_CHAR(\"%s\", '%s') AS \"%s%s\"",
            c.name,
            STRING_DATE_FORMAT.replace(":", "::"),
            c.name,
            stringDateSuffix);
        } else {
          return "\"" + c.name + "\"";
        }
      })
      .collect(Collectors.joining(", "));
  }

  private String tidy(final String name) {
    if (tidyTables) {
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
