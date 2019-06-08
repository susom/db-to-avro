package com.github.susom.starr.dbtoavro.jobrunner.entity;

import java.util.ArrayList;
import java.util.List;

public class Table {

  public String catalog;
  public String schema;
  public String name;
  public long bytes;
  public long rows;
  public List<Column> columns = new ArrayList<>();
  public boolean parallelized;

  public Table(String catalog, String schema, String name) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
  }

  public Table(String catalog, String schema, String name, List<Column> columns) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.columns = columns;
  }


}
