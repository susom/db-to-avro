
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

import java.util.List;

/**
 * Simple pojo for storing an immutable job definition
 */
public class Job {

  public final Long id;
  public final List<String> backupFiles;
  public final String backupUri;
  public final String catalog;
  public final List<String> schemas;
  public final String databaseType;
  public final String postSql;
  public final String preSql;
  public final String type;
  public final String destination;

  private Job(long id, List<String> backupFiles, String backupUri, String catalog, List<String> schemas,
      String databaseType,
      String postSql, String preSql, String type, String destination) {
    this.id = id;
    this.backupFiles = backupFiles;
    this.backupUri = backupUri;
    this.catalog = catalog;
    this.schemas = schemas;
    this.databaseType = databaseType;
    this.postSql = postSql;
    this.preSql = preSql;
    this.type = type;
    this.destination = destination;

  }

  public Job(Builder builder) {
    this.id = builder.id;
    this.backupFiles = builder.backupFiles;
    this.backupUri = builder.backupUri;
    this.catalog = builder.catalog;
    this.schemas = builder.schemas;
    this.databaseType = builder.databaseType;
    this.postSql = builder.postSql;
    this.preSql = builder.preSql;
    this.type = builder.type;
    this.destination = builder.destination;
  }

  public static class Builder {

    private Long id;
    private List<String> backupFiles;
    private String backupUri;
    private String catalog;
    private List<String> schemas;
    private String databaseType;
    private String postSql;
    private String preSql;
    private String type;
    private String destination;

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

    public Builder backupUri(String backupUri) {
      this.backupUri = backupUri;
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

    public Builder databaseType(String databaseType) {
      this.databaseType = databaseType;
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

    public Builder type(String type) {
      this.type = type;
      return this;
    }

    public Builder destination(String destination) {
      this.destination = destination;
      return this;
    }

  }
}
