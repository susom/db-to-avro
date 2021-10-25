package com.github.susom.starr.dbtoavro.entity;

import java.util.List;

/**
 * Simple pojo representing an introspected database table
 */
public class Query {

  public final transient Table table;
  public final String catalog;
  public final String schema;
  public final String name;
  public transient final List<Column> columns;
  public final String query;
  public final String id;
  public final String startRowid;
  public final String endRowid;
  public final int tableQueryCount;

  public Query(Table table, String query, String id, String startRowid, String endRowid) {
    this.table = table;
    this.catalog = table.getCatalog();
    this.schema = table.getSchema();
    this.name = table.getName();
    this.columns = table.getColumns();
    this.tableQueryCount = table.getQueryCount();
    this.query = query;
    this.id = id;
    this.startRowid = startRowid;
    this.endRowid = endRowid;
  }

  public String getQuery() {
    return this.query;
  }

  public String getCatalog() {
    return this.catalog;
  }

  public String getSchema() {
    return this.schema;
  }

  public List<Column> getColumns() {
    return this.columns;
  }

  public String getName() {
    return this.name;
  }

  public String getId() {
    return this.id;
  }

  public String getStartRowid() {
    return this.startRowid;
  }

  public String getEndRowid() {
    return this.endRowid;
  }

}
