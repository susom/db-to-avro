package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import io.reactivex.Completable;
import io.reactivex.Observable;
import java.util.Arrays;

public class SqlServerDockerFns extends DockerFns {

  public SqlServerDockerFns(Config config) {
    super(config);
    this.env = Arrays.asList(config.getStringOrThrow("sqlserver.env").split("\\s*,\\s*"));
  }

  /**
   * {@inheritDoc} Uses sqlcmd utility
   */
  @Override
  public Observable<ConsoleOutput> execSqlShell(final String containerId, final String query) {
    return dockerService.exec(containerId,
        "/opt/mssql-tools/bin/sqlcmd",
        "-s", "localhost",
        "-U", config.getString("database.user"),
        "-P", config.getString("database.password"),
        "-Q", query);
  }

  @Override
  public Completable healthCheck(final String containerId) {
    return execSqlShell(containerId, "SELECT 1;")
        .filter(p -> p.getData().contains("1 rows affected"))
        .count()
        .flatMapCompletable(count -> {
          if (count > 0) {
            return Completable.complete();
          } else {
            return Completable.error(new IllegalArgumentException("Health check failed"));
          }
        });

  }

  @Override
  public String getImage() {
    return config.getStringOrThrow("sqlserver.image");
  }

}
