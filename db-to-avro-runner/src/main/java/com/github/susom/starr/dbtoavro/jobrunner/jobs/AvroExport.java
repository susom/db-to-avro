package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerAvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import io.reactivex.Observable;

public abstract class AvroExport {

  protected final Config config;
  protected final Database database;
  protected final AvroFns avroFns;
  protected final String filePattern;
  protected final int rowsPerAvro;

  public AvroExport(Database database, Config config) {
    this.config = config;
    this.database = database;
    rowsPerAvro = config.getInteger("avro.rows", 100000);
    filePattern = config.getString("avro.filename", "%{SCHEMA}.%{TABLE}-%{PART}.avro");

    switch (database.flavor) {
      case sqlserver:
        avroFns = new SqlServerAvroFns(Config.from()
            .value("database.url", config.getString("sqlserver.url"))
            .value("database.username", config.getString("sqlserver.username"))
            .value("database.password", config.getString("sqlserver.password"))
            .get());
        break;
      case oracle:
        // TBI
      default:
        throw new UnsupportedOperationException("This database is not currently supported for avro export");
    }

  }

  /**
   * Creates avro files based on job specifications
   *
   * @param job job definition
   * @param logger job logger
   * @return Completable containing result or error
   */
  public abstract Observable<AvroFile> run(Job job, JobLogger logger);

}
