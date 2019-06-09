package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Represents a segment of a table which has been split into partitions
 */
public class Partition {

  public Table table;
  public String path;
  public String sql;
  public long part;

  public Partition(Table table, String path, String sql, long part) {
    this.table = table;
    this.path = path;
    this.sql = sql;
    this.part = part;
  }

}
