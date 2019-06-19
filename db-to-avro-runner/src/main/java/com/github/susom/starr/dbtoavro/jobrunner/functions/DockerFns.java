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

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.jobrunner.docker.DockerService;
import com.github.susom.starr.dbtoavro.jobrunner.docker.impl.DockerServiceImpl;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.exceptions.Exceptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

/**
 * Manages an database container that is running in docker
 */
public abstract class DockerFns {

  protected Config config;
  protected DockerService dockerService;
  protected List<String> env;

  public DockerFns(final Config config) {
    this.config = config;
    this.dockerService = new DockerServiceImpl(config);
  }

  /**
   * Creates a database instance running in docker
   *
   * @return containerId of running container
   */
  public Single<String> create(List<String> mounts, List<String> ports) {
    return Single.create(emitter -> {
      try {
        emitter.onSuccess(dockerService.createContainer(getImage(), mounts, env, ports));
      } catch (Exception ex) {
        Exceptions.propagate(ex);
      }
    });
  }

  /**
   * Starts a database container
   *
   * @param containerId containerId where database is running
   * @return completable
   */
  public Completable start(final String containerId) {
    return Completable.create(emitter -> {
      try {
        this.dockerService.startContainer(containerId);
        emitter.onComplete();
      } catch (Exception ex) {
        Exceptions.propagate(ex);
      }
    });
  }

  /**
   * Stops the database container
   *
   * @param containerId containerId where SQL server is running
   * @return completable
   */
  public Completable stop(final String containerId) {
    return Completable.create(emitter -> {
      try {
        this.dockerService.stopContainer(containerId);
        emitter.onComplete();
      } catch (Exception ex) {
        Exceptions.propagate(ex);
      }
    });
  }

  /**
   * Deletes a database container
   *
   * @param containerId containerId to delete
   * @return completable
   */
  public Completable destroy(final String containerId) {
    return Completable.create(emitter -> {
      try {
        this.dockerService.removeContainer(containerId);
        emitter.onComplete();
      } catch (Exception ex) {
        Exceptions.propagate(ex);
      }
    });
  }


  /**
   * Execute a program within a container
   *
   * @param containerId container ID
   * @param cmd program and parameters
   * @return observable of program output
   */
  public Observable<ConsoleOutput> exec(final String containerId, final String... cmd) {
    return dockerService.exec(containerId, cmd);
  }

  /**
   * Executes an SQL statement by calling the native command-line utility inside the docker container. Used for database
   * restores.
   *
   * @param containerId containerId where SQL server is running
   * @param query SQL query to run (should be a simple query)
   * @return an observable with the console output of sqlcmd
   */
  abstract public Observable<ConsoleOutput> execSqlShell(final String containerId, final String query);

  /**
   * Executes an SQL file by calling the native command-line utility inside the docker container. Used for database
   * restores.
   *
   * @param containerId containerId where SQL server is running
   * @param inputFile SQL file to execute
   * @return an observable with the console output of sqlcmd
   */
  public Observable<ConsoleOutput> execSqlShell(final String containerId, File inputFile) {
    if (inputFile == null) {
      return Observable.empty();
    }
    String contents = null;
    try {
      contents = new String(Files.readAllBytes(inputFile.toPath()));
    } catch (IOException ex) {
      Exceptions.propagate(ex);
    }
    return execSqlShell(containerId, contents);
  }

  /**
   * Returns a successful completable if the database container is up and running
   *
   * @param containerId containerId where SQL server is running
   * @return completable, complete if SQL server is up and running
   */
  abstract public Completable healthCheck(final String containerId);

  /**
   * Returns database container image name appropriate for the implementation
   *
   * @return docker container image name
   */
  abstract public String getImage();

}
