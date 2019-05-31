package com.github.susom.starr.dbtoavro.jobrunner.entity;

import com.github.susom.starr.dbtoavro.jobrunner.entity.Database.Catalog.Schema.Table;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple pojo representing an completed Avro export (full or partial table)
 */
public class AvroFile {

  public Table table;
  public String path;
  public String startTime;
  public String endTime;

  public List<String> includedRows = new ArrayList<>();
  public List<String> omittedRows = new ArrayList<>();

  public AvroFile(Table table) {
    this.table = table;
  }

}
