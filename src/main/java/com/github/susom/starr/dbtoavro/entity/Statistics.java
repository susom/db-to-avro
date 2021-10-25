package com.github.susom.starr.dbtoavro.entity;

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.Locale;

public class Statistics {

  private String status;
  private String table;
  private int tableQueryCount;
  private String queryId;
  private int queryFileCount;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private long timeTakenInSeconds;
  private long totalBytes;
  private long exportRowCount;
  private Long dbRowCount;
  private String query;

  public Statistics(String status, String table, int tableQueryCount, String queryId, int queryFileCount, LocalDateTime startTime, LocalDateTime endTime,
    long timeTakenInSeconds, long totalBytes, long exportRowCount, Long dbRowCount, String query) {
    this.status = status;
    this.table = table;
    this.tableQueryCount = tableQueryCount;
    this.queryId = queryId;
    this.queryFileCount = queryFileCount;
    this.startTime = startTime;
    this.endTime = endTime;
    this.timeTakenInSeconds = timeTakenInSeconds;
    this.totalBytes = totalBytes;
    this.exportRowCount = exportRowCount;
    this.dbRowCount = dbRowCount;
    this.query = query;
  }

  public Statistics(String status, String table, int tableQueryCount, String queryId, LocalDateTime startTime, Long dbRowCount, String query) {
    this.status = status;
    this.table = table;
    this.tableQueryCount = tableQueryCount;
    this.queryId = queryId;
    this.startTime = startTime;
    this.dbRowCount = dbRowCount;
    this.query = query;
  }

  public Statistics(String status, String table, int tableQueryCount, LocalDateTime startTime, Long dbRowCount) {
    this.status = status;
    this.table = table;
    this.tableQueryCount = tableQueryCount;
    this.startTime = startTime;
    this.dbRowCount = dbRowCount;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "Query %s: Table=%s, TableQueryCount=%s, QueryId=%s, QueryFileCount=%s, StartTime=%s, EndTime=%s, TimeTaken(seconds)=%s, TotalBytes=%s, ExportRowCount=%s, DbRowCount=%s, Query=\"%s\"", 
      status, table, tableQueryCount, queryId, queryFileCount, 
      startTime != null ? startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "", 
      endTime != null ? endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "",
      timeTakenInSeconds, totalBytes, exportRowCount, dbRowCount, query);
  }

}
