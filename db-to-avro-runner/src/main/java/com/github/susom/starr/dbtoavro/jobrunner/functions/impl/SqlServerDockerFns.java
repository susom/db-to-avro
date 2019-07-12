package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import io.reactivex.Completable;
import io.reactivex.Observable;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqlServerDockerFns extends DockerFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDockerFns.class);

  public SqlServerDockerFns(Config config) {
    super(config);
    this.env = Arrays.asList(config.getStringOrThrow("sqlserver.env").split("\\s*,\\s*"));
  }

  /**
   * {@inheritDoc} Uses sqlcmd utility
   */
  @Override
  public Observable<ConsoleOutput> execSqlFile(final String containerId, final String path) {
    if (path == null) {
      return Observable.empty();
    }
    LOGGER.debug("Executing SQL in {}", path);
    return dockerService.exec(containerId,
        "/opt/mssql-tools/bin/sqlcmd",
        "-s", "localhost",
        "-U", config.getString("database.user"),
        "-P", config.getString("database.password"),
        "-i", path);
  }

  /**
   * {@inheritDoc} Uses sqlcmd utility
   */
  @Override
  public Observable<ConsoleOutput> execSql(final String containerId, final String sql) {
    if (sql == null) {
      return Observable.empty();
    }
    LOGGER.debug("Executing SQL in {}", sql);
    return dockerService.exec(containerId,
        "/opt/mssql-tools/bin/sqlcmd",
        "-s", "localhost",
        "-U", config.getString("database.user"),
        "-P", config.getString("database.password"),
        "-q", sql);
  }


  @Override
  public String getImage() {
    return config.getStringOrThrow("sqlserver.image");
  }

}
