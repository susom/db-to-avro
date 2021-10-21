package com.github.susom.starr.dbtoavro.entity;

import java.util.List;

/**
 * Simple pojo representing a table exported as one or more avro files
 */
public class AvroFile {

  public Table table;
  public Query queryObject;
  public String tableName;
  public String query;
  public List<String> files;
  public long exportTimeMs;
  public long totalBytes;
  public long exportRowCount;
  public Statistics statistics;
  public String queryId;
  public boolean success;

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
    this.success = true;
  }

  public AvroFile(Query query, boolean success) {
    this.queryObject = query;
    this.table = query.table;
    this.query = query.query;
    this.queryId = query.id;
    this.success = success;
  }

  public Query getQueryObject() {
    return queryObject;
  }

}
