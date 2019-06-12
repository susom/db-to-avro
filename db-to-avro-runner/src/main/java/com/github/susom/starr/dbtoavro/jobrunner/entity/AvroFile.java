package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Simple pojo representing an Avro export from an SQL query
 */
public class AvroFile {

  public Table table;
  public String path;
  public String startTime;
  public String endTime;
  public String sql;
  public long bytes;
  public long divisor;

  public AvroFile(Table table, String sql, String path, long divisor) {
    this.table = table;
    this.sql = sql;
    this.path = path;
    this.divisor = divisor;
  }

  public AvroFile(Table table, String sql, String path) {
    this.table = table;
    this.sql = sql;
    this.path = path;
  }


}
