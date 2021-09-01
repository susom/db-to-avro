package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Column;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Single;
import org.apache.avro.file.CodecFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class OracleAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleAvroFns.class);

  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean tidyTables;
  private String filenamePattern;
  private String destination;
  private int avroSize;

  public OracleAvroFns(Job job, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = job.fetchRows;
    this.codec = CodecFactory.fromString(job.codec);
    this.tidyTables = job.tidyTables;
    this.avroSize = job.avroSize;
    this.filenamePattern = job.filenamePattern;
    this.destination = job.destination;
  }

  private AvroFile processSql(long startTime, String path, Etl.SaveAsAvro avro, Table table, String sql) {
    List<String> files = new ArrayList<>();
    long totalRows = 0;
    long totalBytes = 0;
    LOGGER.info("Writing {}", path);
    if (avroSize > 0) {
      Map<String, Long> output = avro.start(avroSize);
      for (Map.Entry<String, Long> entry : output.entrySet()) {
        files.add(entry.getKey());
        totalRows += entry.getValue();
        totalBytes += new File(entry.getKey()).length();
      }
    } else {
      totalRows = avro.start();
      totalBytes = new File(path).length();
      files.add(path);
    }
    
    return new AvroFile(table, sql, files, (System.nanoTime() - startTime) / 1000000, totalBytes, totalRows);

  } 

  public Single<AvroFile> saveAsAvro(final Query sql) {

    Table table = new Table(sql.getCatalog(), sql.getSchema(), sql.getName(), sql.getColumns());
    
    //This has been moved to DatabaseFns for Oracle - getQueries. The queries will NOT be created if the table has no exportable columns 
    /*
    if (sql.getColumns().stream().noneMatch(Column::isExportable)) {
     LOGGER.warn("Skipping table {}, no columns are exportable", sql.getName());
     return Single.just(new AvroFile(table, null, new ArrayList<>(), 0, 0, 0));
    }
    */

    return dbb.transactRx(db -> {

      long startTime = System.nanoTime();

      String path = filenamePattern
      .replace("%{CATALOG}", sql.getCatalog() == null ? "catalog" : tidy(sql.getCatalog()))
      .replace("%{SCHEMA}", sql.getSchema() == null ? "schema" : tidy(sql.getSchema()))
      .replace("%{TABLE}", tidy(sql.getName()) + (StringUtils.isEmpty(sql.id) ? "" : "-" + sql.id));

      Etl.SaveAsAvro avro = Etl.saveQuery(db.get().toSelect(sql.query))
        .asAvro(Paths.get(destination, path).toString(), sql.getSchema(), sql.getName())
        .withCodec(codec)
        .fetchSize(fetchSize);
      
      return processSql(startTime, path, avro, table, sql.getQuery());

    }).toSingle();
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
