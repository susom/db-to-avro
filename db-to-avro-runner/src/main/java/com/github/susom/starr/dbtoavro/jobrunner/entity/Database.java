package com.github.susom.starr.dbtoavro.jobrunner.entity;

import com.github.susom.database.Flavor;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple pojo describing a restored database server running in docker
 */
public class Database {

  public String containerId;
  public Flavor flavor;
  public List<Catalog> catalogs = new ArrayList<>();

  public Database(String containerId) {
    this.containerId = containerId;
  }

  public class Catalog {

    public String name;
    public List<Schema> schemas = new ArrayList<>();

    public Catalog(String name) {
      this.name = name;
    }

    public Database getDatabase() {
      return Database.this;
    }

    public class Schema {

      public String name;
      public List<Table> tables = new ArrayList<>();

      public Schema(String name) {
        this.name = name;
      }

      public Catalog getCatalog() {
        return Catalog.this;
      }

      public class Table {

        public String name;
        public transient String tempName;
        public long rows;
        public List<Column> columns = new ArrayList<>();

        public Table(String name) {
          this.name = name;
        }

        public Schema getSchema() {
          return Schema.this;
        }

        public class Column {

          public String name;
          public int type;

          public Column(String name, int type) {
            this.name = name;
            this.type = type;
          }

        }

      }

    }
  }
}
