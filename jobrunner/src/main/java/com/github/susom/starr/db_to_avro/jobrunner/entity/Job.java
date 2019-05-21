
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

package com.github.susom.starr.db_to_avro.jobrunner.entity;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.Serializable;
import java.util.List;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "backupFiles",
    "backupUri",
    "databaseName",
    "databaseType",
    "failed",
    "id",
    "postSql",
    "preSql",
    "tables",
    "type"
})
public class Job implements Serializable {

  private final static long serialVersionUID = 1148138198057396362L;
  @JsonProperty("backupFiles")
  private List<String> backupFiles = null;
  @JsonProperty("backupUri")
  private String backupUri;
  @JsonProperty("databaseName")
  private String databaseName;
  @JsonProperty("databaseType")
  private String databaseType;
  @JsonProperty("failed")
  private Boolean failed;
  @JsonProperty("id")
  private Long id;
  @JsonProperty("postSql")
  private String postSql;
  @JsonProperty("preSql")
  private String preSql;
  @JsonProperty("tables")
  private List<Table> tables = null;
  @JsonProperty("type")
  private String type;

  /**
   * No args constructor for use in serialization
   */
  public Job() {
  }

  /**
   * Defines everything needed to run a particular job
   */
  public Job(List<String> backupFiles, String backupUri, String databaseName, String databaseType, Boolean failed,
      Long id, String postSql, String preSql, List<Table> tables, String type) {
    super();
    this.backupFiles = backupFiles;
    this.backupUri = backupUri;
    this.databaseName = databaseName;
    this.databaseType = databaseType;
    this.failed = failed;
    this.id = id;
    this.postSql = postSql;
    this.preSql = preSql;
    this.tables = tables;
    this.type = type;
  }

  @JsonProperty("backupFiles")
  public List<String> getBackupFiles() {
    return backupFiles;
  }

  @JsonProperty("backupFiles")
  public void setBackupFiles(List<String> backupFiles) {
    this.backupFiles = backupFiles;
  }

  public Job withBackupFiles(List<String> backupFiles) {
    this.backupFiles = backupFiles;
    return this;
  }

  @JsonProperty("backupUri")
  public String getBackupUri() {
    return backupUri;
  }

  @JsonProperty("backupUri")
  public void setBackupUri(String backupUri) {
    this.backupUri = backupUri;
  }

  public Job withBackupUri(String backupUri) {
    this.backupUri = backupUri;
    return this;
  }

  @JsonProperty("databaseName")
  public String getDatabaseName() {
    return databaseName;
  }

  @JsonProperty("databaseName")
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public Job withDatabaseName(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  @JsonProperty("databaseType")
  public String getDatabaseType() {
    return databaseType;
  }

  @JsonProperty("databaseType")
  public void setDatabaseType(String databaseType) {
    this.databaseType = databaseType;
  }

  public Job withDatabaseType(String databaseType) {
    this.databaseType = databaseType;
    return this;
  }

  @JsonProperty("failed")
  public Boolean getFailed() {
    return failed;
  }

  @JsonProperty("failed")
  public void setFailed(Boolean failed) {
    this.failed = failed;
  }

  public Job withFailed(Boolean failed) {
    this.failed = failed;
    return this;
  }

  @JsonProperty("id")
  public Long getId() {
    return id;
  }

  @JsonProperty("id")
  public void setId(Long id) {
    this.id = id;
  }

  public Job withId(Long id) {
    this.id = id;
    return this;
  }

  @JsonProperty("postSql")
  public String getPostSql() {
    return postSql;
  }

  @JsonProperty("postSql")
  public void setPostSql(String postSql) {
    this.postSql = postSql;
  }

  public Job withPostSql(String postSql) {
    this.postSql = postSql;
    return this;
  }

  @JsonProperty("preSql")
  public String getPreSql() {
    return preSql;
  }

  @JsonProperty("preSql")
  public void setPreSql(String preSql) {
    this.preSql = preSql;
  }

  public Job withPreSql(String preSql) {
    this.preSql = preSql;
    return this;
  }

  @JsonProperty("tables")
  public List<Table> getTables() {
    return tables;
  }

  @JsonProperty("tables")
  public void setTables(List<Table> tables) {
    this.tables = tables;
  }

  public Job withTables(List<Table> tables) {
    this.tables = tables;
    return this;
  }

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  public Job withType(String type) {
    this.type = type;
    return this;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(tables).append(id).append(databaseType).append(databaseName).append(preSql)
        .append(backupFiles).append(failed).append(backupUri).append(type).append(postSql).toHashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if ((other instanceof Job) == false) {
      return false;
    }
    Job rhs = ((Job) other);
    return new EqualsBuilder().append(tables, rhs.tables).append(id, rhs.id).append(databaseType, rhs.databaseType)
        .append(databaseName, rhs.databaseName).append(preSql, rhs.preSql).append(backupFiles, rhs.backupFiles)
        .append(failed, rhs.failed).append(backupUri, rhs.backupUri).append(type, rhs.type).append(postSql, rhs.postSql)
        .isEquals();
  }

}
