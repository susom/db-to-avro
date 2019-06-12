package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Simple pojo representing a database query
 */
public class Query {

  public Table table;
  public String sql;
  public long divisor; // hint for exporter, how many rows per avro file
  public long part;    // file number if split into multiple files

  public Query(Table table, String sql, long divisor, long part) {
    this.table = table;
    this.sql = sql;
    this.divisor = divisor;
    this.part = part;
  }

  public Query(Table table, String sql, long divisor) {
    this.table = table;
    this.sql = sql;
    this.divisor = divisor;
  }
}
