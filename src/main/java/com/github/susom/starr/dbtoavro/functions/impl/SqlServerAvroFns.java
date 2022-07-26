package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Statistics;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.database.DatabaseProvider;
import io.reactivex.Single;
import org.apache.avro.file.CodecFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SqlServerAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerAvroFns.class);

  private final DatabaseProvider.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean tidyTables;
  private String filenamePattern;
  private String destination;
  private int avroSize;

  public SqlServerAvroFns(Job job, DatabaseProvider.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = job.fetchRows;
    this.codec = CodecFactory.fromString(job.codec);
    this.tidyTables = job.tidyTables;
    this.avroSize = job.avroSize;
    this.filenamePattern = job.filenamePattern;
    this.destination = job.destination;
  }

  @Override
  public Single<AvroFile> saveAsAvro(final Query queryObject) {
    return Single.fromCallable(
        () ->
            dbb.transactReturning(
                db -> {
                  Table table = queryObject.table;
                  long startTime = System.nanoTime();
                  LocalDateTime startLocalTime = LocalDateTime.now();

                  String path =
                      filenamePattern
                          .replace(
                              "%{CATALOG}",
                              queryObject.getCatalog() == null
                                  ? "catalog"
                                  : tidy(queryObject.getCatalog()))
                          .replace(
                              "%{SCHEMA}",
                              queryObject.getSchema() == null
                                  ? "schema"
                                  : tidy(queryObject.getSchema()))
                          .replace(
                              "%{TABLE}",
                              tidy(queryObject.getName())
                                  + (StringUtils.isEmpty(queryObject.id)
                                      ? ""
                                      : "-" + queryObject.id));

                  LOGGER.info(
                      "{}",
                      new Statistics(
                          "Started",
                          table.getName(),
                          queryObject.tableQueryCount,
                          queryObject.getId(),
                          startLocalTime,
                          table.getDbRowCount(),
                          queryObject.getQuery()));

                  Etl.SaveAsAvro avro =
                      Etl.saveQuery(db.get().toSelect(queryObject.query))
                          .asAvro(
                              Paths.get(destination, path).toString(),
                              queryObject.getSchema(),
                              queryObject.getName())
                          .withCodec(codec)
                          .fetchSize(fetchSize);
                  return processSql(startLocalTime, startTime, path, avro, queryObject);
                }));
  }

  private AvroFile processSql(
      LocalDateTime startLocalTime,
      long startTime,
      String path,
      Etl.SaveAsAvro avro,
      Query queryObject) {

    String query = queryObject.getQuery();
    String queryId = queryObject.getId();

    List<String> files = new ArrayList<>();
    long exportRowCount = 0;
    long totalBytes = 0;
    LOGGER.info("Writing {} for queryId {}, query is {}", path, queryId, query);
    if (avroSize > 0) {
      Map<String, Long> output = avro.start(avroSize);
      for (Map.Entry<String, Long> entry : output.entrySet()) {
        files.add(entry.getKey());
        exportRowCount += entry.getValue();
        totalBytes += new File(entry.getKey()).length();
      }
    } else {
      exportRowCount = avro.start();
      totalBytes = new File(path).length();
      files.add(path);
    }
    long endTime = System.nanoTime();
    LocalDateTime endLocalTime = LocalDateTime.now();
    Table table = queryObject.table;
    Statistics statistics =
        new Statistics(
            "Completed",
            table.getName(),
            queryObject.tableQueryCount,
            queryId,
            files.size(),
            startLocalTime,
            endLocalTime,
            Duration.between(startLocalTime, endLocalTime).getSeconds(),
            totalBytes,
            exportRowCount,
            table.getDbRowCount(),
            query);
    LOGGER.info("{}", statistics);
    return new AvroFile(
        queryObject,
        files,
        (endTime - startTime) / 1000000,
        totalBytes,
        exportRowCount,
        statistics);
  }

  private String tidy(final String name) {
    if (name != null && tidyTables) {
      return name.replaceAll("[^a-zA-Z0-9]", " ")
          .replaceAll("\\s", "_")
          .trim()
          .toLowerCase(Locale.ROOT);
    } else {
      return name;
    }
  }
}
