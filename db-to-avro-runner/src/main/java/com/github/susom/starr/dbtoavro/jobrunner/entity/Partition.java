package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Represents a segment of a table which has been split into partitions
 */
public class Partition {

  public Table table;
  public String path;
  public String sql;

  public Partition(Table table, String path, String sql) {
    this.table = table;
    this.path = path;
    this.sql = sql;
  }

}
