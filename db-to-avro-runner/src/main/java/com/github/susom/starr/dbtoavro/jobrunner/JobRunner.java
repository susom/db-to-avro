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

package com.github.susom.starr.dbtoavro.jobrunner;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.AvroExporter;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.OracleLoadDataPump;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.OracleLoadExisting;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.SqlServerLoadBackup;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.SqlServerLoadExisting;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.reactivex.Completable;
import java.io.File;
import java.io.PrintStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master job runner
 */
public class JobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  private final Job job;
  private final Config config;
  private final DatabaseProviderRx.Builder dbb;

  public JobRunner(Config config, Job job) {
    this.config = config;
    this.job = job;
    this.dbb = DatabaseProviderRx
        .pooledBuilder(config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();
  }

  /**
   * Entry point for running jobs
   *
   * @return completable
   */
  public Completable run() {

    String outputFile = config.getString("avro.logfile", "log.json");

    Loader loader;
    switch (job.flavor) {
      case sqlserver:
        if (job.connection == null) {
          loader = new SqlServerLoadBackup(config, dbb);
        } else {
          loader = new SqlServerLoadExisting(config, dbb);
        }
        break;
      case oracle:
        if (job.connection == null) {
          loader = new OracleLoadDataPump(config, dbb);
        } else {
          loader = new OracleLoadExisting(config, dbb);
        }
        break;
      default:
        return Completable.error(new IllegalArgumentException("Unimplemented database " + job.flavor));
    }

    if (job.destination != null) {
      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      return new AvroExporter(config, dbb).run(job, loader)
          .toList()
          .doOnSuccess(avro -> {
            job.avro = avro;
            try (PrintStream ps = new PrintStream(job.destination + File.separator + outputFile)) {
              ps.println(gson.toJson(job));
            }
            LOGGER.info("Wrote output to {}", outputFile);
          })
          .ignoreElement();
    } else {
      LOGGER.info("No destination, not exporting avro");
      return loader.run(job).ignoreElement();
    }

  }

}
