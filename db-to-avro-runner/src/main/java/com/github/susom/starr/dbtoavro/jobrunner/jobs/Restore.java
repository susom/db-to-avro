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

package com.github.susom.starr.dbtoavro.jobrunner.jobs;

import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import com.github.susom.starr.dbtoavro.jobrunner.util.RetryWithDelay;
import io.reactivex.Maybe;
import io.reactivex.schedulers.Schedulers;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class Restore {

  /**
   * Restores a database backup into a docker container
   *
   * @param docker implementation of{@link DockerFns}
   * @param sql implementation of{@link DatabaseFns}
   * @param job job information
   * @param logger job logger
   * @return Completable containing result or error
   */
  public static Maybe<Result> run(DockerFns docker, DatabaseFns sql, Job job, JobLogger logger) {

    return docker.create().flatMapMaybe(containerId ->

        docker.start(containerId)
            .doOnComplete(
                () -> logger.log(
                    String.format(Locale.CANADA, "Container %s started, waiting for database to boot", containerId)))
            .andThen(docker.healthCheck(containerId).retryWhen(new RetryWithDelay(3, 2000)))

            .andThen(sql.transact(job.preSql))
            .doOnComplete(() -> logger.log("Database pre-sql completed"))

            .doOnComplete(() -> logger.log("Starting database restore"))
            .andThen(sql.getRestoreSql(job.databaseName, Arrays.asList(job.backupFiles.split(",")))
                .flatMapObservable(ddl ->
                    docker.execSqlShell(containerId, ddl)
                        .observeOn(Schedulers.io())))
            .filter(p -> p.getPercent() != -1)
            .doOnNext(p -> logger.progress(p.getPercent(), 100)).ignoreElements()
            .doOnComplete(() -> logger.log("Restore completed"))

            .andThen(sql.transact(job.postSql))
            .doOnComplete(() -> logger.log("Database post-sql completed"))

            .doOnComplete(() -> logger.log("Introspecting schema"))
            .andThen(sql.getTables(job.databaseName))
            .map(tables -> new Result(containerId, tables)));

  }

  /**
   * Encapsulates results of running the above fn
   */
  public static class Result {

    String containerId;
    List<Table> tables;

    Result(String containerId, List<Table> tables) {
      this.containerId = containerId;
      this.tables = tables;
    }

  }

}
