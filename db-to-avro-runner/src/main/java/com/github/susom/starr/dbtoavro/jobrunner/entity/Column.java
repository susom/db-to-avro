package com.github.susom.starr.dbtoavro.jobrunner.entity;

/**
 * Simple pojo representing a column in a table
 */
public class Column {

  public String name;
  public int type;
  public String typeName;
  public boolean primaryKey;
  public boolean serializable; // if false, this column will not be exported

  public Column(String name, int type, String typeName, boolean serializable) {
    this.name = name;
    this.type = type;
    this.typeName = typeName;
    this.serializable = serializable;
  }

}
