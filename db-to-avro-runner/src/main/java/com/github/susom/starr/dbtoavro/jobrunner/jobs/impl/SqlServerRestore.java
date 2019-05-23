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
import com.github.susom.starr.dbtoavro.jobrunner.docker.impl.DockerServiceImpl;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerDockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Restore;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import com.github.susom.starr.dbtoavro.jobrunner.util.RetryWithDelay;
import io.reactivex.Maybe;
import io.reactivex.schedulers.Schedulers;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class SqlServerRestore extends Restore {

  private SqlServerDockerFns docker;
  private SqlServerDatabaseFns db;

  public SqlServerRestore(Config config) {
    super(config);

    Config sqlServerConfig = Config.from()
        .value("database.url", config.getString("sqlserver.url"))
        .value("database.username", config.getString("sqlserver.username"))
        .value("database.password", config.getString("sqlserver.password"))
        .get();

    db = new SqlServerDatabaseFns(sqlServerConfig);
  }

  @Override
  public Maybe<Result> run(Job job, JobLogger logger) {

    // Mount the backup source directory to /backup on the docker container
    List<String> mounts = new ArrayList<>();
    mounts.add(new File(job.backupUri) + ":/backup");
    docker = new SqlServerDockerFns(new DockerServiceImpl(config), config, mounts);

    return docker.create().flatMapMaybe(containerId ->
        docker.start(containerId)
            .doOnComplete(() -> logger
                .log(String.format(Locale.CANADA, "Container %s started, waiting for database to boot", containerId)))
            .andThen(docker.healthCheck(containerId).retryWhen(new RetryWithDelay(3, 2000)))

            .andThen(db.transact(job.preSql))
            .doOnComplete(() -> logger.log("Database pre-sql completed"))

            .doOnComplete(() -> logger.log("Starting database restore"))

            .andThen(db.getRestoreSql(job.databaseName, Arrays.asList(job.backupFiles.split(",")))
                .flatMapObservable(ddl ->
                    docker.execSqlShell(containerId, ddl)
                        .observeOn(Schedulers.io()))
                .filter(p -> p.getPercent() != -1)
                .doOnNext(p -> logger.progress(p.getPercent(), 100)).ignoreElements()
                .doOnComplete(() -> logger.log("Restore completed"))

                .andThen(db.transact(job.postSql))
                .doOnComplete(() -> logger.log("Database post-sql completed"))

                .andThen(db
                    .getTables(job.databaseName))
                .map(tables -> new Result(containerId, tables))
            )
    );
  }

}
