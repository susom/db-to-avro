package com.github.susom.starr.dbtoavro.entity;

/**
 * Simple pojo representing a future database query
 */
public class Query {

  public Table table;
  public String sql;
  public String path;
  public long batchSize;

  public Query(Table table, String sql, long batchSize, String path) {
    this.table = table;
    this.sql = sql;
    this.batchSize = batchSize;
    this.path = path;
  }

}
