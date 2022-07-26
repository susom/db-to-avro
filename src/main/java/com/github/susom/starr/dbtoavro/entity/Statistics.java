package com.github.susom.starr.dbtoavro.entity;

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

public class Statistics {

  private String status;
  private transient String table;
  private transient int tableQueryCount;
  private String queryId;
  private int queryFileCount;
  private LocalDateTime startTime;
  private LocalDateTime endTime;
  private long timeTakenInSeconds;
  private long totalBytes;
  private long exportRowCount;
  private transient Long dbRowCount;
  private transient String query;
  private List<String> files;

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

  public String getStatus() {return status;}
  public String getTable() {return table;}
  public int getTableQueryCount() {return tableQueryCount;}
  public String getQueryId() {return queryId;}
  public int getQueryFileCount() {return queryFileCount;}
  public LocalDateTime getStartTime() {return startTime;}
  public LocalDateTime getEndTime() {return endTime;}
  public long getTimeTakenInSeconds() {return timeTakenInSeconds;}
  public long getTotalBytes() {return totalBytes;}
  public long getExportRowCount() {return exportRowCount;}
  public Long getDbRowCount() {return dbRowCount;}
  public String getQuery() {return query;}
  public List<String> getFiles() {return files;}
  public void setFiles(List<String> files) {this.files = files;}

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "Query %s: Table=%s, TableQueryCount=%s, QueryId=%s, QueryFileCount=%s, StartTime=%s, EndTime=%s, TimeTaken(seconds)=%s, TotalBytes=%s, ExportRowCount=%s, DbRowCount=%s, Query=\"%s\"", 
      status, table, tableQueryCount, queryId, queryFileCount, 
      startTime != null ? startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "", 
      endTime != null ? endTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) : "",
      timeTakenInSeconds, totalBytes, exportRowCount, dbRowCount, query);
  }

}
