package com.github.susom.starr.dbtoavro.entity;

//import com.google.gson.annotations.Expose;
import java.util.List;

/**
 * Simple pojo representing an introspected database table
 */
public class Table {

  private String catalog;
  private String schema;
  private String name;
  private transient List<Column> columns;
  private long dbRowCount;
  private int queryCount;
  private List<SplitTableStrategy> splitStrategies;

  public Table(String catalog, String schema, String name, List<Column> columns) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.columns = columns;
  }

  public Table(String catalog, String schema, String name, List<Column> columns, Long dbRowCount, List<SplitTableStrategy> splitStrategies) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.columns = columns;
    this.dbRowCount = dbRowCount;
    this.splitStrategies = splitStrategies;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getDbRowCount() {
    return dbRowCount;
  }

  public void setDbRowCount(long dbRowCount) {
    this.dbRowCount = dbRowCount;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public List<SplitTableStrategy> getSplitStrategies() {
    return splitStrategies;
  }

  public void setSplitStrategies(List<SplitTableStrategy> splitStrategies) {
    this.splitStrategies = splitStrategies;
  }

  public int getQueryCount() {
    return queryCount;
  }

  public void setQueryCount(int queryCount) {
    this.queryCount = queryCount;
  }

}
