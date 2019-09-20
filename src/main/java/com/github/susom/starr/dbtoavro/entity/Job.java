
/*
 * Copyright 2019 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.github.susom.starr.dbtoavro.entity;

import com.github.susom.database.Flavor;
import java.util.List;

/**
 * Simple pojo for storing an immutable job definition
 */
public class Job {

  public final String id;
  public final List<String> backupFiles;
  public final String backupDir;
  public final String catalog;
  public final List<String> schemas;
  public final List<String> tables;
  public final List<String> exclusions;
  public final String postSql;
  public final String preSql;
  public final Flavor flavor;
  public final String destination;
  public final String connection;
  public final String timezone;
  public final Boolean stringDate;
  public final String stringDateFormat;
  public final String stringDateSuffix;
  public final int fetchRows;
  public final int avroSize;
  public final String codec;
  public final boolean optimized;
  public final boolean tidyTables;


  // Job files
  public List<AvroFile> avro;

  public Job(Builder builder) {
    this.id = builder.id;
    this.backupFiles = builder.backupFiles;
    this.backupDir = builder.backupDir;
    this.catalog = builder.catalog;
    this.schemas = builder.schemas;
    this.tables = builder.tables;
    this.exclusions = builder.exclusions;
    this.postSql = builder.postSql;
    this.preSql = builder.preSql;
    this.flavor = builder.flavor;
    this.destination = builder.destination;
    this.connection = builder.connection;
    this.timezone = builder.timezone;
    this.stringDate = builder.stringDate;
    this.stringDateFormat = builder.stringDateFormat;
    this.stringDateSuffix = builder.stringDateSuffix;
    this.fetchRows = builder.fetchRows;
    this.avroSize = builder.avroSize;
    this.codec = builder.codec;
    this.optimized = builder.optimized;
    this.tidyTables = builder.tidyTables;
  }

  public static class Builder {

    private String id;
    private List<String> backupFiles;
    private String backupDir;
    private String catalog;
    private List<String> schemas;
    private List<String> tables;
    private List<String> exclusions;
    private String postSql;
    private String preSql;
    private Flavor flavor;
    private String destination;
    private String connection;
    private String timezone;
    private Boolean stringDate;
    private String stringDateFormat;
    private String stringDateSuffix;
    private int fetchRows;
    private int avroSize;
    private String codec;
    private boolean optimized;
    private boolean tidyTables;

    public Builder() {
    }

    public Job build() {
      return new Job(this);
    }

    public Builder id(String id) {
      this.id = id;
      return this;
    }

    public Builder backupFiles(List<String> backupFiles) {
      this.backupFiles = backupFiles;
      return this;
    }

    public Builder backupDir(String backupUri) {
      this.backupDir = backupUri;
      return this;
    }

    public Builder catalog(String catalog) {
      this.catalog = catalog;
      return this;
    }

    public Builder schemas(List<String> schemas) {
      this.schemas = schemas;
      return this;
    }

    public Builder tables(List<String> tables) {
      this.tables = tables;
      return this;
    }

    public Builder exclusions(List<String> exclusions) {
      this.exclusions = exclusions;
      return this;
    }

    public Builder postSql(String postSql) {
      this.postSql = postSql;
      return this;
    }

    public Builder preSql(String preSql) {
      this.preSql = preSql;
      return this;
    }

    public Builder flavor(Flavor type) {
      this.flavor = type;
      return this;
    }

    public Builder destination(String destination) {
      this.destination = destination;
      return this;
    }

    public Builder connection(String connection) {
      this.connection = connection;
      return this;
    }

    public Builder timezone(String timezone) {
      this.timezone = timezone;
      return this;
    }

    public Builder stringDate(boolean stringDate) {
      this.stringDate = stringDate;
      return this;
    }

    public Builder stringDateFormat(String stringDateFormat) {
      this.stringDateFormat = stringDateFormat;
      return this;
    }

    public Builder stringDateSuffix(String stringDateSuffix) {
      this.stringDateSuffix = stringDateSuffix;
      return this;
    }

    public Builder fetchRows(int fetchRows) {
      this.fetchRows = fetchRows;
      return this;
    }

    public Builder avroSize(int avroSize) {
      this.avroSize = avroSize;
      return this;
    }

    public Builder codec(String codec) {
      this.codec = codec;
      return this;
    }

    public Builder optimized(boolean optimized) {
      this.optimized = optimized;
      return this;
    }

    public Builder tidyTables(boolean tidyTables) {
      this.tidyTables = tidyTables;
      return this;
    }

  }
}
