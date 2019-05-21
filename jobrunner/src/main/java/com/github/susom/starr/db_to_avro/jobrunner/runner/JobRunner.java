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

package com.github.susom.starr.db_to_avro.jobrunner.runner;

import com.github.susom.starr.db_to_avro.jobrunner.docker.DockerService;
import com.github.susom.starr.db_to_avro.jobrunner.docker.impl.DockerServiceImpl;
import com.github.susom.starr.db_to_avro.jobrunner.entity.Job;
import com.github.susom.starr.db_to_avro.jobrunner.functions.DockerFns;
import com.github.susom.starr.db_to_avro.jobrunner.functions.impl.SqlServerDockerFns;
import com.github.susom.starr.db_to_avro.jobrunner.functions.impl.SqlServerFns;
import com.github.susom.starr.db_to_avro.jobrunner.jobs.SqlServer;
import com.github.susom.database.Config;
import io.reactivex.Completable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master job runner
 */
public abstract class JobRunner implements JobLogger {

  private static Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  protected Job job;
  private Config config;

  private DockerService dockerService;

  public JobRunner(Config config, Job job) {
    this.config = config;
    this.job = job;
    dockerService = new DockerServiceImpl(config);
  }

  /**
   * Entrypoint for running different job types
   * @return completable
   */
  public Completable run() {

    switch (job.getType()) {

      case "restore":

        if (job.getDatabaseType().equals("sql-server")) {
          try {
            List<String> mounts = new ArrayList<>();
            mounts.add(new File(job.getBackupUri()) + ":/backup");

            DockerFns docker = new SqlServerDockerFns(dockerService, config, mounts);

            Config sqlServerConfig = Config.from()
                .value("database.url", config.getString("sqlserver.url"))
                .value("database.username", config.getString("sqlserver.username"))
                .value("database.password", config.getString("sqlserver.password"))
                .get();

            SqlServerFns sql = new SqlServerFns(sqlServerConfig);

            log("Starting SqlServer restore task");
            return SqlServer.Restore(docker, sql, job, this);

          } catch (Exception ex) {
            return Completable.error(ex);
          }
        } else {
          return Completable.error(new IllegalArgumentException("Unknown database type " + job));
        }

      default:
        return Completable.error(new IllegalArgumentException("Unknown job type " + job.getDatabaseType()));

    }

  }

}
