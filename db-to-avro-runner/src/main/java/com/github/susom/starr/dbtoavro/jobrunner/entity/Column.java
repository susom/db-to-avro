package com.github.susom.starr.dbtoavro.jobrunner.entity;

public class Column {

  public String name;
  public int type;
  public String typeName;
  public boolean primaryKey;
  public boolean serializable;

  public Column(String name, int type, String typeName, boolean serializable) {
    this.name = name;
    this.type = type;
    this.typeName = typeName;
    this.serializable = serializable;
  }

}
