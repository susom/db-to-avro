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

package com.github.susom.starr.dbtoavro.jobrunner.docker.impl;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateNetworkResponse;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.PingCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Network;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.StreamType;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.github.dockerjava.core.util.CompressArchiveUtil;
import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.jobrunner.docker.DockerService;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.exceptions.Exceptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.ws.rs.ProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods for interacting with a docker server running on the localhost
 */
public class DockerServiceImpl implements DockerService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DockerServiceImpl.class);
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

  @Override
  public String createContainer(final String image, final List<String> mounts, final List<String> env,
      final List<String> ports) {
    // Uses same '/host_path:/container_path' syntax as command line docker
    Volume[] volumes = new Volume[mounts.size()];
    Bind[] binds = new Bind[mounts.size()];

    for (int i = 0; i < mounts.size(); i++) {
      volumes[i] = new Volume(mounts.get(i).split(":")[1]);
      binds[i] = new Bind(mounts.get(i).split(":")[0], volumes[i]);
    }

    Ports portBindings = new Ports();
    ports.forEach(port -> portBindings.bind(
        ExposedPort.tcp(Integer.valueOf(port.split(":")[0])),
        Ports.Binding.bindPort(Integer.valueOf(port.split(":")[1])))
    );

    return
        dockerClient
            .createContainerCmd(image)
            .withHostConfig(
                new HostConfig()
                    .withBinds(binds)
                    .withPortBindings(portBindings)
                    .withNetworkMode("db-to-avro"))
            .withIpv4Address("10.10.10.100")
            .withVolumes(volumes)
            .withName("database")
            .withHostName("database")
            .withEnv(env.toArray(new String[0]))
            .withLabels(new HashMap<String, String>() {{
              put("creator", "db-to-avro");
            }})
            .exec().getId();
  }

  @Override
  public void startContainer(final String containerId) {
    dockerClient.startContainerCmd(containerId).exec();
  }

  /**
   * Stops a container with the given containerId
   */
  @Override
  public void stopContainer(final String containerId) {
    dockerClient.stopContainerCmd(containerId).exec();
  }

  @Override
  public void removeContainer(final String containerId) {
    dockerClient.removeContainerCmd(containerId).exec();
  }

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

  @Override
  public void createFileFromString(String containerId, String filename, String contents) {
    try {
      Path tempFile = Files.createTempDirectory(null).resolve(filename);
      try (PrintWriter out = new PrintWriter(tempFile.toFile())) {
        out.println(contents);
      }
      Path outputTarFile = Files.createTempFile(null, null);
      LOGGER.debug("Wrote file in container: {}", tempFile.toAbsolutePath());
      CompressArchiveUtil.tar(tempFile, outputTarFile, true, false);
      try (InputStream uploadStream = Files.newInputStream(outputTarFile)) {
        dockerClient.copyArchiveToContainerCmd(containerId)
            .withTarInputStream(uploadStream)
            .exec();
      }
    } catch (IOException ex) {
      Exceptions.propagate(ex);
    }
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
