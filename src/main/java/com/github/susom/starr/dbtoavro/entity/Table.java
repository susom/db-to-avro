package com.github.susom.starr.dbtoavro.entity;
import java.util.List;

/**
 * Simple pojo representing an introspected database table
 */
public class Table {

  public final String catalog;
  public final String schema;
  public final String name;
  public final List<Column> columns;

  public Table(String catalog, String schema, String name, List<Column> columns) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.columns = columns;
  }

}
