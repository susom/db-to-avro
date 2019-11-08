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

package com.github.susom.starr.dbtoavro.functions;

import com.github.dockerjava.api.model.Container;
import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.docker.DockerService;
import com.github.susom.starr.dbtoavro.docker.impl.DockerServiceImpl;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.exceptions.Exceptions;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages an database container that is running in docker
 */
public abstract class DockerFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerFns.class);

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
        emitter.onError(ex);
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
        emitter.onError(ex);
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
        emitter.onError(ex);

      }
    });
  }

  /**
   * Execute a program within a container
   *
   * @param containerId container ID
   * @param cmd program and parameters
   * @return observable of program files
   */
  public Observable<ConsoleOutput> exec(final String containerId, final String... cmd) {
    return dockerService.exec(containerId, cmd);
  }

  /**
   * Executes an SQL statement by calling the native command-line utility inside the docker container. Used for database
   * restores. Path is the path *inside* the container.
   *
   * @param containerId containerId where SQL server is running
   * @param path fully qualified path of SQL file to execute
   * @return an observable with the console files of sqlcmd
   */
  public abstract Observable<ConsoleOutput> execSqlFile(final String containerId, final String path);

  /**
   * Executes an SQL statement by calling the native command-line utility inside the docker container. Used for database
   * restores.
   *
   * @param containerId containerId where SQL server is running
   * @param sql SQL to execute
   * @return an observable with the console files of sqlcmd
   */
  public abstract Observable<ConsoleOutput> execSql(final String containerId, final String sql);

  /**
   * Checks if a container is running
   *
   * @param containerId containerId
   * @return completable, complete if container is running
   */
  public Single<Boolean> isRunning(final String containerId) {
    return Single.create(emitter -> {
      try {
        boolean status = dockerService.listContainers().stream().anyMatch(c -> c.getId().equals(containerId));
        LOGGER.debug("Container {} running: {}", containerId.substring(0, 12), status);
        emitter.onSuccess(dockerService.listContainers().stream().anyMatch(c -> c.getId().equals(containerId)));
      } catch (Exception ex) {
        emitter.onError(ex);
      }
    });
  }

  /**
   * Checks if a container is running and healthy
   *
   * @param containerId containerId
   * @return completable, complete if container is running
   */
  public Single<Boolean> isHealthy(final String containerId) {
    return Single.create(emitter -> {
      try {
        List<Container> containers = dockerService.listContainers();
        for (Container container : containers) {
          if (container.getId().equals(containerId)) {
            String state = container.getStatus();
            LOGGER.debug("Container {} {}", containerId.substring(0, 12), state);
            if (state.contains("healthy") || state.contains("Up")) {
              emitter.onSuccess(Boolean.TRUE);
            }
          }
        }
        emitter.onSuccess(Boolean.FALSE);
      } catch (Exception ex) {
        emitter.onError(ex);
      }
    });
  }

  /**
   * Returns database container image name appropriate for the implementation
   *
   * @return docker container image name
   */
  public abstract String getImage();

}
