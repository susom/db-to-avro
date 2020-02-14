package com.github.susom.starr.dbtoavro.entity;

/**
 * Simple pojo representing a column in a table
 */
public class Column {

  public final String name;
  public final int jdbcType;
  public final String vendorType;
  public boolean primaryKey;
  public final boolean supported;
  public final boolean excluded;

  public Column(String name, int jdbcType, String vendorType, boolean supported, boolean excluded) {
    this.name = name;
    this.jdbcType = jdbcType;
    this.vendorType = vendorType;
    this.supported = supported;
    this.excluded = excluded;
  }

  public boolean isExportable() {
    return (this.supported && !this.excluded);
  }

}
