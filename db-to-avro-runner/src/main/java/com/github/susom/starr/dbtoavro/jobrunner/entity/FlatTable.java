package com.github.susom.starr.dbtoavro.jobrunner.entity;

import java.util.ArrayList;
import java.util.List;

public class FlatTable {

  public String catalog;
  public String schema;
  public String name;
  public long rows;
  public long bytes;
  public List<Column> columns = new ArrayList<>();

  public static class Column {

    public String name;
    public int type;
    public boolean isPk;
    public short ordinal;

    public Column(String name, int type) {
      this.name = name;
      this.type = type;
    }

    public short getOrdinal(){
      return ordinal;
    }

  }

}
