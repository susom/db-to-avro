
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
  public final List<String> tablesSplit;
  public final List<String> tablePriorities;
  public final List<String> unionizeQuery;
  public final List<String> tableExclusions;
  public final List<String> columnExclusions;
  public final String postSql;
  public final String preSql;
  public final Flavor flavor;
  public final String destination;
  public final String logfile;
  public final String timezone;
  public final Boolean stringDatetime;
  public final String stringDatetimeSuffix;
  public final String filenamePattern;
  public final int fetchRows;
  public final int avroSize;
  public final String codec;
  public final boolean tidyTables;
  public final boolean continueOnException;
  public final List<SplitTableStrategy> splitTableStrategies;
  public final int incrementFactor;

  public transient List<AvroFile> avro;
  public long runtimeMs;
  public List<Output> outputList;

  public Job(Builder builder) {
    this.id = builder.id;
    this.backupFiles = builder.backupFiles;
    this.backupDir = builder.backupDir;
    this.catalog = builder.catalog;
    this.schemas = builder.schemas;
    this.tables = builder.tables;
    this.tablesSplit = builder.tablesSplit;
    this.tablePriorities = builder.tablePriorities;
    this.unionizeQuery = builder.unionizeQuery;
    this.tableExclusions = builder.tableExclusions;
    this.columnExclusions = builder.columnExclusions;
    this.postSql = builder.postSql;
    this.preSql = builder.preSql;
    this.flavor = builder.flavor;
    this.destination = builder.destination;
    this.logfile = builder.logfile;
    this.timezone = builder.timezone;
    this.stringDatetime = builder.stringDatetime;
    this.stringDatetimeSuffix = builder.stringDatetimeSuffix;
    this.filenamePattern = builder.filenamePattern;
    this.fetchRows = builder.fetchRows;
    this.avroSize = builder.avroSize;
    this.codec = builder.codec;
    this.tidyTables = builder.tidyTables;
    this.continueOnException = builder.continueOnException;
    this.splitTableStrategies = builder.splitTableStrategies;
    this.incrementFactor = builder.incrementFactor;
  }

  public void setOutputFromAvro(List<AvroFile> avroList) {
    avro = avroList;
    Map<String, List<AvroFile>> map = avro.stream().filter(w -> w.tableName != null).collect(Collectors.groupingBy(w -> w.tableName));
    outputList = new ArrayList<Output>();
    map.entrySet().stream().forEach(e -> {
      Output output = new Output();
      output.queries = new ArrayList<String>();
      output.statisticsList = new ArrayList<Statistics>();
      output.tableName = e.getKey();
      List<AvroFile> data = e.getValue();
      AvroFile af = data.get(0);
      output.catalog = af.table.getCatalog();
      output.schema = af.table.getSchema();
      output.splitStrategies = af.table.getSplitStrategies();
      //output.columns = af.table.getColumns();
      output.dbRowCount = af.table.getDbRowCount();
      output.tableQueryCount = af.table.getQueryCount();

      output.exportRowCount = 0L;
      output.filesCount = 0;
      output.totalBytes = 0;
      output.startTime = af.statistics.getStartTime();
      output.endTime = af.statistics.getEndTime();

      data.forEach(x -> {
        output.queries.add(String.format("Id- %s: query- %s", x.queryId, x.query));
        output.statisticsList.add(x.statistics);
        output.exportRowCount += x.exportRowCount;
        output.filesCount += x.files.size();
        output.totalBytes += x.totalBytes;
        output.startTime = (x.statistics.getStartTime().isBefore(output.startTime)) ? x.statistics.getStartTime() : output.startTime;
        output.endTime = (x.statistics.getEndTime().isAfter(output.endTime)) ? x.statistics.getEndTime() : output.endTime;
      });
      output.timeTakenInSeconds = Duration.between(output.startTime, output.endTime).getSeconds();
      outputList.add(output);
    });
  }

  public static class Builder {

    private String id;
    private List<String> backupFiles;
    private String backupDir;
    private String catalog;
    private List<String> schemas;
    private List<String> tables;
    private List<String> tablesSplit;
    private List<String> tablePriorities;
    private List<String> unionizeQuery;
    private List<String> tableExclusions;
    private List<String> columnExclusions;
    private String postSql;
    private String preSql;
    private Flavor flavor;
    private String destination;
    private String logfile;
    private String timezone;
    private Boolean stringDatetime;
    private String stringDatetimeSuffix;
    private String filenamePattern;
    private int fetchRows;
    private int avroSize;
    private String codec;
    private boolean tidyTables;
    private boolean continueOnException;
    private List<SplitTableStrategy> splitTableStrategies;
    private int incrementFactor;

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

    public Builder tablesSplit(List<String> tablesSplit) {
      this.tablesSplit = tablesSplit;
      return this;
    }
    
    public Builder tablePriorities(List<String> tablePriorities) {
      this.tablePriorities = tablePriorities;
      return this;
    }

    public Builder unionizeQuery(List<String> unionizeQuery) {
      this.unionizeQuery = unionizeQuery;
      return this;
    }

    public Builder tableExclusions(List<String> tableExclusions) {
      this.tableExclusions = tableExclusions;
      return this;
    }

    public Builder columnExclusions(List<String> columnExclusions) {
      this.columnExclusions = columnExclusions;
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

    public Builder logfile(String logfile) {
      this.logfile = logfile;
      return this;
    }

    public Builder timezone(String timezone) {
      this.timezone = timezone;
      return this;
    }

    public Builder stringDatetime(boolean stringDatetime) {
      this.stringDatetime = stringDatetime;
      return this;
    }

    public Builder stringDatetimeSuffix(String stringDatetimeSuffix) {
      this.stringDatetimeSuffix = stringDatetimeSuffix;
      return this;
    }

    public Builder filenamePattern(String filenamePattern) {
      this.filenamePattern = filenamePattern;
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

    public Builder tidyTables(boolean tidyTables) {
      this.tidyTables = tidyTables;
      return this;
    }

    public Builder continueOnException(boolean continueOnException) {
      this.continueOnException = continueOnException;
      return this;
    }

    public Builder splitTableStrategies(List<SplitTableStrategy> splitTableStrategies) {
      this.splitTableStrategies = splitTableStrategies;
      return this;
    }

    public Builder incrementFactor(int incrementFactor) {
      this.incrementFactor = incrementFactor;
      return this;
    }

  }
}
