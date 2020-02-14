package com.github.susom.starr.dbtoavro.entity;

import java.util.List;

/**
 * Simple pojo representing a table exported as one or more avro files
 */
public class AvroFile {

  public Table table;
  public String query;
  public List<String> files;
  public long exportTimeMs;
  public long totalBytes;
  public long totalRows;

  public AvroFile(Table table, String query, List<String> files, long exportTimeMs, long totalBytes, long totalRows) {
    this.table = table;
    this.query = query;
    this.files = files;
    this.exportTimeMs = exportTimeMs;
    this.totalBytes = totalBytes;
    this.totalRows = totalRows;
  }

}
