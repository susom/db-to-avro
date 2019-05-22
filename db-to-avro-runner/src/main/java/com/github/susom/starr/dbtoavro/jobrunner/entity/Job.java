
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

/**
 * Simple pojo for storing an immutable job definition
 */
public class Job {

  public final Long id;
  public final String backupFiles;
  public final String backupUri;
  public final String databaseName;
  public final String databaseType;
  public final String postSql;
  public final String preSql;
  public final String type;

  public Job(long id, String backupFiles, String backupUri, String databaseName, String databaseType, String postSql,
      String preSql, String type) {
    this.id = id;
    this.backupFiles = backupFiles;
    this.backupUri = backupUri;
    this.databaseName = databaseName;
    this.databaseType = databaseType;
    this.postSql = postSql;
    this.preSql = preSql;
    this.type = type;
  }

  public Job(Builder builder) {
    this.id = builder.id;
    this.backupFiles = builder.backupFiles;
    this.backupUri = builder.backupUri;
    this.databaseName = builder.databaseName;
    this.databaseType = builder.databaseType;
    this.postSql = builder.postSql;
    this.preSql = builder.preSql;
    this.type = builder.type;
  }

  public static class Builder {

    private Long id;
    private String backupFiles;
    private String backupUri;
    private String databaseName;
    private String databaseType;
    private String postSql;
    private String preSql;
    private String type;

    public Builder() {
    }

    public Job build() {
      return new Job(this);
    }

    public Builder id(long id) {
      this.id = id;
      return this;
    }

    public Builder backupFiles(String backupFiles) {
      this.backupFiles = backupFiles;
      return this;
    }

    public Builder backupUri(String backupUri) {
      this.backupUri = backupUri;
      return this;
    }

    public Builder databaseName(String databaseName) {
      this.databaseName = databaseName;
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

  }
}
