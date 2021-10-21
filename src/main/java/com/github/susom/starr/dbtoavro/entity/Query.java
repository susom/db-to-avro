package com.github.susom.starr.dbtoavro.entity;

import java.util.List;

/**
 * Simple pojo representing an introspected database table
 */
public class Query {

  public final String catalog;
  public final String schema;
  public final String name;
  public final List<Column> columns;
  public final String query;
  public final String id;
  public final String startRowid;
  public final String endRowid;
  public final int numberOfQueriesForTable;
  public final Table table;

  public Query(String catalog, String schema, String name, List<Column> columns, String query, String id, String startRowid, String endRowid, int numberOfQueriesForTable, Table table) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.columns = columns;
    this.query = query;
    this.id = id;
    this.startRowid = startRowid;
    this.endRowid = endRowid;
    this.numberOfQueriesForTable = numberOfQueriesForTable;
    this.table = table;
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
