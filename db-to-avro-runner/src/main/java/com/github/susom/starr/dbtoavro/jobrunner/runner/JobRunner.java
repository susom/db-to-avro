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

package com.github.susom.starr.dbtoavro.jobrunner.runner;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerAvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.AvroExport;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.SqlServerLoadExisting;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.SqlServerLoadBackup;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.reactivex.Completable;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master job runner
 */
public abstract class JobRunner implements JobLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  protected Job job;
  private Config config;

  public JobRunner(Config config, Job job) {
    this.config = config;
    this.job = job;
  }

  /**
   * Entry point for running jobs
   *
   * @return completable
   */
  public Completable run() {

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    if (job.flavor.equals("sqlserver")) {

      Single<Warehouse> warehouse;
      AvroFns avroFns = new SqlServerAvroFns(config);

      if (job.connection == null) {
        log("Starting sql server database restore");
        warehouse = new SqlServerLoadBackup(config).run(job, this);
      } else {
        log("Using existing sql server database");
        warehouse = new SqlServerLoadExisting(config).run(job, this);
      }

      return warehouse.flatMapCompletable(wh ->
          new AvroExport(wh, config).run(job, this)
              .toList()
              .map(gson::toJson)
              .doOnSuccess(json -> {
                log("\"AvroFiles\": ");
                log(json);
              })
              .ignoreElement());

    } else {
      return Completable.error(new IllegalArgumentException("Unknown database flavor " + job.flavor));
    }

  }

}
