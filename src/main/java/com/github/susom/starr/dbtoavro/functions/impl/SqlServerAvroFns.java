package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.dbgoodies.etl.Etl;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Column;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import io.reactivex.Single;
import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlServerAvroFns implements AvroFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerAvroFns.class);
  private static int STRING_DATE_CONVERSION = 126;

  private final DatabaseProviderRx.Builder dbb;
  private final int fetchSize;
  private CodecFactory codec;
  private boolean tidyTables;
  private boolean stringDate;
  private String stringDateSuffix;
  private String filenamePattern;
  private String destination;
  private int avroSize;

  public SqlServerAvroFns(Job job, DatabaseProviderRx.Builder dbb) {
    this.dbb = dbb;
    this.fetchSize = job.fetchRows;
    this.codec = CodecFactory.fromString(job.codec);
    this.tidyTables = job.tidyTables;
    this.stringDate = job.stringDatetime;
    this.stringDateSuffix = job.stringDatetimeSuffix;
    this.avroSize = job.avroSize;
    this.filenamePattern = job.filenamePattern;
    this.destination = job.destination;
  }

  @Override
  public Single<AvroFile> saveAsAvro(final Table table) {
    if (table.columns.stream().noneMatch(Column::isExportable)) {
      LOGGER.warn("Skipping table {}, no columns are exportable", table.name);
      return Single.just(new AvroFile(table, null, new ArrayList<>(), 0, 0, 0));
    }
    return dbb.transactRx(db -> {

      long startTime = System.nanoTime();
      db.get().underlyingConnection().setCatalog(table.catalog);

      // Only dump the supported column types
      String columns = getColumnSql(table);

      String sql = String
          .format(Locale.ROOT, "SELECT %s FROM [%s].[%s] WITH (NOLOCK)", columns, table.schema,
            table.name);

      String path = filenamePattern
        .replace("%{CATALOG}", table.catalog == null ? "catalog" : tidy(table.catalog))
        .replace("%{SCHEMA}", table.schema == null ? "schema" : tidy(table.schema))
        .replace("%{TABLE}", tidy(table.name));

      Etl.SaveAsAvro avro = Etl.saveQuery(db.get().toSelect(sql))
        .asAvro(Paths.get(destination, path).toString(), table.schema, table.name)
        .withCodec(codec)
        .fetchSize(fetchSize);

      if (tidyTables) {
        avro = avro.tidyNames();
      }

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
        files.add(path);
      }

      return new AvroFile(table, sql, files, (System.nanoTime() - startTime) / 1000000, totalBytes, totalRows);

    }).toSingle();
  }

  private String getColumnSql(Table table) {
    return table.columns.stream()
      .filter(Column::isExportable)
      .map(col -> {
        // Use column name string not JDBC type val to avoid sqlserver->jdbc mappings
        if (stringDate && (col.vendorType.equals("datetime") || col.vendorType.equals("datetime2") || col.vendorType.equals("smalldatetime"))) {
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
