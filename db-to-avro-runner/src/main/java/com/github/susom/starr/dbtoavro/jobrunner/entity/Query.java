package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Simple pojo representing a future database query
 */
public class Query {

  public Table table;
  public String sql;
  public String path;
  public long rowsPerFile;

  public Query(Table table, String sql, long rowsPerFile, String path) {
    this.table = table;
    this.sql = sql;
    this.rowsPerFile = rowsPerFile;
    this.path = path;
  }

}
