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

package com.github.susom.starr.db_to_avro.jobrunner.jobs;

import com.github.susom.starr.db_to_avro.jobrunner.entity.Job;
import com.github.susom.starr.db_to_avro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.db_to_avro.jobrunner.functions.DockerFns;
import com.github.susom.starr.db_to_avro.jobrunner.runner.JobLogger;
import com.github.susom.starr.db_to_avro.jobrunner.util.RetryWithDelay;
import io.reactivex.Completable;
import io.reactivex.schedulers.Schedulers;

public class SqlServer {

  /**
   * Restores an SQL-Server database backup into a new container
   *
   * @param docker implementation of{@link DockerFns}
   * @param sql implementation of{@link DatabaseFns}
   * @param job job information
   * @param logger job logger
   * @return Completable containing result or error
   */
  public static Completable Restore(DockerFns docker, DatabaseFns sql, Job job, JobLogger logger) {
    return docker.create().flatMapCompletable(containerId ->

        docker.start(containerId)
            .doOnComplete(() -> logger.log(String.format("Container %s started, waiting for database to boot", containerId)))
            .andThen(docker.healthCheck(containerId).retryWhen(new RetryWithDelay(3, 2000)))

            // Execute pre-sql commands
            .andThen(sql.transact(job.getPreSql()))
            .doOnComplete(() -> logger.log("Database pre-sql completed"))

            .doOnComplete(() -> logger.log("Starting database restore"))
            .andThen(sql.getRestoreSql(job.getDatabaseName(), job.getBackupFiles())
                .flatMapObservable(ddl ->
                    docker.execSqlShell(containerId, ddl)
                        .observeOn(Schedulers.io())))
            .filter(p -> p.getPercent() != -1)
            .doOnNext(p -> logger.progress(p.getPercent(), 100)).ignoreElements()
            .doOnComplete(() -> logger.log("Restore completed"))

            .andThen(sql.transact(job.getPostSql()))
            .doOnComplete(() -> logger.log("Database post-sql completed"))

            .doOnComplete(() -> logger.log("Introspecting schema"))
            .andThen(sql.getTables(job.getDatabaseName()))
            .doOnSuccess(job::setTables)
            .ignoreElement()

            .doOnComplete(() -> logger.log(String.format("Stopping container %s", containerId)))
            .andThen(docker.stop(containerId))

            .doOnComplete(() -> logger.log(String.format("Deleting container %s", containerId)))
            .andThen(docker.destroy(containerId))

            .doOnComplete(() -> logger.log("Job complete!")));
  }

}
