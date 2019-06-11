package com.github.susom.starr.dbtoavro.jobrunner.entity;

public class Column {

  public String name;
  public int type;
  public boolean isPrimaryKey;

  public Column(String name, int type) {
    this.name = name;
    this.type = type;
  }

}
