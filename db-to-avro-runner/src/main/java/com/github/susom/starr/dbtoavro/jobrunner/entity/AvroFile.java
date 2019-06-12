package com.github.susom.starr.dbtoavro.jobrunner.entity;

import java.util.List;

/**
 * Simple pojo representing the result of running a query
 */
public class AvroFile {

  public Query query;
  public List<String> output;
  public String startTime;
  public String endTime;
  public long bytes;

  public AvroFile(Query query, List<String> output, String startTime, String endTime, long bytes) {
    this.query = query;
    this.output = output;
    this.startTime = startTime;
    this.endTime = endTime;
    this.bytes = bytes;
  }

}
