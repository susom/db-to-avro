package com.github.susom.starr.dbtoavro.entity;

import java.time.LocalDateTime;
import java.util.List;

public class Output {

  public String tableName;
  public String catalog;
  public String schema;
  public List<SplitTableStrategy> splitStrategies;
  //public List<Column> columns;
  public List<String> queries;
  public List<Statistics> statisticsList;

  public Long dbRowCount;
  public Long exportRowCount;
  public int tableQueryCount;
  public int filesCount;
  public long totalBytes;
  public LocalDateTime startTime;
  public LocalDateTime endTime;
  public long timeTakenInSeconds;

  /*   
  class QuerySummary {
    String id;
    String query;
  }
 */
}
