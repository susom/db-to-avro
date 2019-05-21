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

package com.github.susom.starr.db_to_avro.jobrunner.docker.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.PingCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.susom.starr.db_to_avro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.db_to_avro.jobrunner.docker.DockerService;
import com.github.susom.database.Config;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods for interacting with a docker server running on the localhost
 */
public class DockerServiceImpl implements DockerService {

  private static Logger LOGGER = LoggerFactory.getLogger(DockerServiceImpl.class);
  private DockerClient dockerClient;
  private String socket;

  private StringBuilder stdinBuffer;
  private StringBuilder stdoutBuffer;
  private StringBuilder stderrBuffer;

  public DockerServiceImpl(Config config) {
    this.socket = config.getString("docker.host", "unix:///var/run/docker.sock");
    this.stdinBuffer = new StringBuilder();
    this.stdoutBuffer = new StringBuilder();
    this.stderrBuffer = new StringBuilder();
    connect();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String createContainer(final String image, final List<String> mounts, final List<String> env) {
    // Uses same '/host_path:/container_path' syntax as command line docker
    Volume[] volumes = new Volume[mounts.size()];
    Bind[] binds = new Bind[mounts.size()];

    for (int i = 0; i < mounts.size(); i++) {
      volumes[i] = new Volume(mounts.get(i).split(":")[1]);
      binds[i] = new Bind(mounts.get(i).split(":")[0], volumes[i]);
    }

    ExposedPort tcp1433 = ExposedPort.tcp(1433);
    Ports portBindings = new Ports();
    portBindings.bind(tcp1433, Ports.Binding.bindPort(1433));

    return
        dockerClient
            .createContainerCmd(image)
            .withVolumes(volumes)
            .withEnv(env.toArray(new String[0]))
            .withHostConfig(new HostConfig().withBinds(binds).withPortBindings(portBindings))
            .exec().getId();

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void startContainer(final String containerId) {
    dockerClient.startContainerCmd(containerId).exec();
  }

  /**
   * Stops a container with the given containerId
   * @param containerId
   */
  @Override
  public void stopContainer(final String containerId) {
    dockerClient.stopContainerCmd(containerId).exec();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeContainer(final String containerId) {
    dockerClient.removeContainerCmd(containerId).exec();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Observable<ConsoleOutput> exec(final String containerId, final String... cmd) {
    return Observable.create(emitter -> {
      ExecCreateCmdResponse execCreateCmdResponse =
          dockerClient
              .execCreateCmd(containerId)
              .withAttachStdout(true)
              .withAttachStderr(true)
              .withCmd(cmd).exec();
      dockerClient
          .execStartCmd(execCreateCmdResponse.getId())
          .withDetach(false)
          .withTty(false)
          .exec(getResultCallback(emitter))
          .awaitCompletion();
      emitter.onComplete();
    });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Observable<ConsoleOutput> logs(final String containerId, boolean follow, int numberOfLines) {
    return Observable.create(emitter -> {
      dockerClient.logContainerCmd(containerId)
          .withStdOut(true)
          .withStdErr(true)
          .withFollowStream(follow)
          .withTail(numberOfLines)
          .exec(getResultCallback(emitter))
          .awaitCompletion();
      emitter.onComplete();
    });
  }

  private StringBuilder getBuffer(final StreamType streamType) throws IOException {
    switch (streamType) {
      case STDIN:
        return stdinBuffer;
      case RAW:
      case STDOUT:
        return stdoutBuffer;
      case STDERR:
        return stderrBuffer;
      default:
        throw new IOException("Unknown docker stream");
    }
  }


  private ExecStartResultCallback getResultCallback(ObservableEmitter<ConsoleOutput> emitter) {
    return new ExecStartResultCallback() {
      @Override
      public void onNext(Frame frame) {
        super.onNext(frame);
        List<ConsoleOutput> result = new ArrayList<>();
        try {
          StringBuilder currentLine = getBuffer(frame.getStreamType());
          String out = new String(frame.getPayload(), StandardCharsets.UTF_8);
          for (int i = 0; i < out.length(); i++) {
            char split = out.charAt(i);
            if (split == '\n') {
              result.add(new ConsoleOutput(frame.getStreamType(), currentLine.toString()));
              currentLine.setLength(0);
            } else {
              currentLine.append(split);
            }
          }
        } catch (Exception ex) {
          emitter.tryOnError(ex);
        }
        result.forEach(emitter::onNext);
      }
    };
  }

  private void connect() {
    if (dockerClient != null) {
      return;
    }
    LOGGER.info("Connecting to docker at {}", socket);
    DockerClientConfig clientConfig =
        DefaultDockerClientConfig.createDefaultConfigBuilder()
            .withDockerHost(socket)
            .build();
    this.dockerClient = DockerClientBuilder.getInstance(clientConfig).build();
    try {
      PingCmd pingCmd = this.dockerClient.pingCmd();
      pingCmd.exec();
    } catch (ProcessingException ex) {
      if (ex.getCause() instanceof ConnectException) {
        throw new IllegalStateException("Docker server is not running!", ex);
      }
    }
  }



}
