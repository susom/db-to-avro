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

package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerDockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.util.RetryWithDelay;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restores an SQL server database from a backup file
 */
public class SqlServerLoadBackup implements Loader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerLoadBackup.class);

  private DockerFns docker;
  private SqlServerDatabaseFns db;
  private Config config;

  public SqlServerLoadBackup(Config config) {
    this.config = config;
    this.db = new SqlServerDatabaseFns(config);
  }

  @Override
  public Single<Database> run(Job job) {

    LOGGER.info("Starting sql server database restore");

    // Mount the backup source directory to /backup on the docker container
    List<String> mounts = new ArrayList<>();
    mounts.add(new File(job.backupDir) + ":/backup");

    docker = new SqlServerDockerFns(config);

    return docker.create(mounts).flatMap(containerId ->
        docker.start(containerId)
            .doOnComplete(() -> LOGGER
                .info(String.format(Locale.CANADA, "Container %s started, waiting for database to boot", containerId)))
            .andThen(docker.healthCheck(containerId).retryWhen(new RetryWithDelay(3, 2000)))
            .andThen(db.transact(job.preSql))
            .doOnComplete(() -> LOGGER.info("Database pre-sql completed"))
            .doOnComplete(() -> LOGGER.info("Starting database restore"))
            .andThen(db.getRestoreSql(job.catalog, job.backupFiles)
                .flatMapObservable(ddl ->
                    docker.execSqlShell(containerId, ddl)
                        .observeOn(Schedulers.io()))
                .doOnNext(p -> LOGGER.info(p.getData()))
                .ignoreElements()
                .doOnComplete(() -> LOGGER.info("Restore completed"))
                .andThen(db.transact(job.postSql))
                .doOnComplete(() -> LOGGER.info("Database post-sql completed"))
                .andThen(db.getDatabase(containerId))
                .doFinally(() -> LOGGER.info("Database introspection complete"))
            )
    );
  }

}
