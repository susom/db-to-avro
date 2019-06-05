package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Stores two SQL objects which represent the upper (inclusive) and lower (exclusive) bounding ranges of a table extract
 */
public class BoundedRange {

  public SqlObject lower;
  public SqlObject upper;
  public long index;
  public boolean terminal;

  public BoundedRange(int index) {
    this.index = index;
  }

  public BoundedRange(SqlObject lower, SqlObject upper, int index) {
    this.lower = lower;
    this.upper = upper;
    this.index = index;
  }

  public static class SqlObject {

    private String columnName;
    private Object value;

    public SqlObject(String name, Object value) {
      this.columnName = name;
      this.value = value;
    }

    public Object getValue() {
      return value;
    }

    public String getName() {
      return columnName;
    }

  }

}
