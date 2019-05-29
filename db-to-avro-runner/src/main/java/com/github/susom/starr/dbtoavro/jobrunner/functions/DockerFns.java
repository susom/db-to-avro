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

package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface DockerFns {

  /**
   * Creates an SQL server instance running in docker
   *
   * @return containerId of running container
   */
  Single<String> create();

  /**
   * Starts an SQL container
   *
   * @param containerId containerId where SQL server is running
   * @return completable
   */
  Completable start(final String containerId);

  /**
   * Stops an SQL container
   *
   * @param containerId containerId where SQL server is running
   * @return completable
   */
  Completable stop(final String containerId);

  /**
   * Deletes an SQL server container
   *
   * @param containerId containerId to delete
   * @return completable
   */
  Completable destroy(final String containerId);

  /**
   * Returns a successful completable if the SQL server is up and running
   *
   * @param containerId containerId where SQL server is running
   * @return completable, complete if SQL server is up and running
   */
  Completable healthCheck(final String containerId);

  /**
   * Executes an SQL statement by calling the 'sqlcmd' utility in the docker container. Used for database restores.
   *
   * @param containerId containerId where SQL server is running
   * @param query SQL query to run (should be a simple query)
   * @return an observable with the console output of sqlcmd
   */
  Observable<ConsoleOutput> execSqlShell(final String containerId, final String query);

}
