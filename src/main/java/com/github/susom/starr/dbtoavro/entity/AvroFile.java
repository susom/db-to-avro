package com.github.susom.starr.dbtoavro.entity;

import java.util.List;

/**
 * Simple pojo representing a table exported as one or more avro files
 */
public class AvroFile {

  public Query query;
  public List<String> files;
  public String startTime;
  public String endTime;
  public long bytes;
  public long rows;

  public AvroFile(Query query, List<String> files, String startTime, String endTime, long bytes, long rows) {
    this.query = query;
    this.files = files;
    this.startTime = startTime;
    this.endTime = endTime;
    this.bytes = bytes;
    this.rows = rows;
  }

}
