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
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.impl.SqlServerRestore;
import com.google.gson.GsonBuilder;
import io.reactivex.Completable;
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
   * Entrypoint for running different job types
   *
   * @return completable
   */
  public Completable run() {

    switch (job.type) {

      case "restore":

        if (job.databaseType.equals("sql-server")) {
          try {

            log("Starting SqlServer restore task");
            return new SqlServerRestore(config).run(job, this)
                .doOnSuccess(restoreResult -> {
                  log("\"RestoreResult\": ");
                  log(new GsonBuilder().setPrettyPrinting().create().toJson(restoreResult));
                })
                .ignoreElement();

          } catch (Exception ex) {
            return Completable.error(ex);
          }
        } else {
          return Completable.error(new IllegalArgumentException("Unknown database type " + job));
        }

      default:
        return Completable.error(new IllegalArgumentException("Unknown job type " + job.databaseType));

    }

  }

}
