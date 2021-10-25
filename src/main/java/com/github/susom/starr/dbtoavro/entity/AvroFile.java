package com.github.susom.starr.dbtoavro.entity;

import java.util.List;

/**
 * Simple pojo representing a table exported as one or more avro files
 */
public class AvroFile {

  public transient Table table;
  public transient Query queryObject;
  public String tableName;
  public String query;
  public List<String> files;
  public long exportTimeMs;
  public long totalBytes;
  public long exportRowCount;
  public Statistics statistics;
  public String queryId;

  public AvroFile(Query queryObject, List<String> files, long exportTimeMs, long totalBytes, long exportRowCount, Statistics statistics) {
    this.tableName = queryObject.table.getName();
    this.table = queryObject.table;
    this.query = queryObject.query;
    this.files = files;
    this.exportTimeMs = exportTimeMs;
    this.totalBytes = totalBytes;
    this.exportRowCount = exportRowCount;
    this.queryId = queryObject.id;
    this.statistics = statistics;
  }

  public Query getQueryObject() {
    return queryObject;
  }

}
