
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

package com.github.susom.starr.dbtoavro.jobrunner.entity;

import com.github.susom.database.Flavor;
import java.io.File;
import java.util.List;

/**
 * Simple pojo for storing an immutable job definition
 */
public class Job {

  public final Long id;
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
  }

  public static class Builder {

    private Long id;
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

    public Builder() {
    }

    public Job build() {
      return new Job(this);
    }

    public Builder id(long id) {
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

  }
}
