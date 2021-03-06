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

package com.github.susom.starr.dbtoavro.docker;

import com.github.dockerjava.api.model.Container;
import io.reactivex.Observable;
import java.util.List;

public interface DockerService {

  /**
   * Creates (but does not start) a docker container
   * @param image docker image to create container from
   * @param mounts paths to mount to docker container in form /source:/dest,...
   * @param env environment variables to pass to docker container, comma delimited key=value pairs
   * @param ports to open to container, in the form port:port,...
   * @return the containerId of the new container
   */
  String createContainer(String image, List<String> mounts, List<String> env, List<String> ports);

  /**
   * Starts a container with the given containerId
   */
  void startContainer(String containerId);

  /**
   * Stops a container with the given containerId
   */
  void stopContainer(String containerId);

  /**
   * Deletes a container with the given containerId
   */
  void removeContainer(String containerId);

  /**
   * Gets list of running containers
   */
  List<Container> listContainers();

  /**
   * Executes a command within the provided container, returning an observable watching the files. Observable completes
   * when command exits.
   * @param containerId containerId to execute commands
   * @param cmd command to pass to container
   */
  Observable<ConsoleOutput> exec(String containerId, String... cmd);

  /**
   * Returns an observable that returns logs from the given docker container
   * @param containerId containerId for logs
   * @param follow keep following logs
   * @param numberOfLines number of lines to retrieve
   * @return observable of notify files
   */
  Observable<ConsoleOutput> logs(String containerId, boolean follow, int numberOfLines);

  /**
   * Creates a file inside a given container ID with contents
   * @param contents string contents for file
   * @param filename filename for file
   */
  void createFileFromString(String containerId, String filename, String contents);

}

