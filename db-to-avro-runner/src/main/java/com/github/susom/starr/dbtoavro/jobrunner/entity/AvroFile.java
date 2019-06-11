package com.github.susom.starr.dbtoavro.jobrunner.entity;

import java.util.List;

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
  public long rowsPerFile;

  public List<String> includedColumns;
  public List<String> excludedColumns;

  public AvroFile(Table table, String sql, String path, List<String> includedColumns, List<String> excludedColumns, long rowsPerFile) {
    this.table = table;
    this.sql = sql;
    this.path = path;
    this.includedColumns = includedColumns;
    this.excludedColumns = excludedColumns;
    this.rowsPerFile = rowsPerFile;
  }

  public AvroFile(Table table, String sql, String path, List<String> includedColumns, List<String> excludedColumns) {
    this.table = table;
    this.sql = sql;
    this.path = path;
    this.includedColumns = includedColumns;
    this.excludedColumns = excludedColumns;
  }


}
