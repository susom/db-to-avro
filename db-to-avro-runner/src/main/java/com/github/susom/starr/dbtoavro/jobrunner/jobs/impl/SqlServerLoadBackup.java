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
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restores an SQL server database from a native backup (.bak) file(s)
 */
public class SqlServerLoadBackup implements Loader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerLoadBackup.class);

  private DockerFns docker;
  private SqlServerDatabaseFns db;
  private Config config;
  private String containerId;

  public SqlServerLoadBackup(Config config, DatabaseProviderRx.Builder dbb) {
    this.config = config;
    this.db = new SqlServerDatabaseFns(config, dbb);
  }

  @Override
  public Single<Database> run(Job job) {

    LOGGER.info("Starting sql server database restore");

    // Mount the backup source directory to /backup on the docker container
    List<String> mounts = new ArrayList<>();
    mounts.add(new File(job.backupDir) + ":/backup");
    if (config.getString("sqlserver.mounts") != null) {
      mounts.addAll(Arrays.asList(config.getStringOrThrow("sqlserver.mounts").split("\\s*,\\s*")));
    }
    List<String> ports = Arrays.asList(config.getString("sqlserver.ports", "1433:1433").split("\\s*,\\s*"));

    docker = new SqlServerDockerFns(config);

    return docker.create(mounts, ports).flatMap(containerId -> {
          this.containerId = containerId;
          return docker.start(containerId)
              .andThen(docker.isRunning(containerId)
                  .delay(5, TimeUnit.SECONDS)
                  .repeat()
                  .takeWhile(b -> b.equals(false))
                  .ignoreElements()
              )
              .andThen(docker.isHealthy(containerId)
                  .delay(5, TimeUnit.SECONDS)
                  .repeat()
                  .takeWhile(b -> b.equals(false))
                  .ignoreElements()
              )
              .andThen(db.isValid()
                  .delay(5, TimeUnit.SECONDS)
                  .repeat()
                  .takeWhile(b -> b.equals(false))
                  .ignoreElements()
              )
              .andThen(docker.execSqlFile(containerId, job.preSql))
              .doOnNext(line -> LOGGER.info(line.getData()))
              .ignoreElements()
              .doOnComplete(() -> LOGGER.info("Starting database restore"))
              .andThen(
                  db.getRestoreSql(job.catalog, job.backupFiles)
                      .flatMapObservable(ddl ->
                          docker.execSql(containerId, ddl)
                              .observeOn(Schedulers.io()))
                      .doOnNext(p -> LOGGER.info(p.getData()))
                      .ignoreElements()
                      .doOnComplete(() -> LOGGER.info("Restore completed"))
                      .andThen(docker.execSqlFile(containerId, job.postSql))
                      .doOnNext(line -> LOGGER.info(line.getData()))
                      .doOnComplete(() -> LOGGER.info("Database pre-sql completed"))
                      .ignoreElements()
                      .andThen(db.getDatabase(containerId))
                      .doFinally(() -> LOGGER.info("Database introspection complete"))
              );
        }
    );
  }

  @Override
  public Completable stop() {
    return new SqlServerDockerFns(config).stop(this.containerId);
  }

}
