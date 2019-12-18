package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.functions.DockerFns;
import io.reactivex.Completable;
import io.reactivex.Observable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleDockerFns extends DockerFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleDockerFns.class);

  private final int impdpThreads;

  public OracleDockerFns(Config config) {
    super(config);
    this.env = Arrays.asList(config.getStringOrThrow("oracle.env").split("\\s*,\\s*"));
    this.impdpThreads = (int) (Runtime.getRuntime().availableProcessors() * (config
        .getDouble("oracle.impdp.core.multiplier", 1.0)));
  }

  public Observable<ConsoleOutput> impdp(final String containerId, final List<String> backupFiles) {
    if (backupFiles.size() != 1) {
      return Observable.error(new Throwable("Oracle impdp requires a single .par file."));
    }
    return dockerService.exec(containerId,
        "impdp",
        String.format(Locale.ROOT, "userid=%s/%s@//0.0.0.0:1521/ORCLPDB1", config.getString("database.user"),
            config.getString("database.password")),
        (impdpThreads > 0) ? "PARALLEL=" + impdpThreads : "",
        "PARFILE=/backup/" + backupFiles.get(0)
    );
  }

  /**
   * {@inheritDoc} Uses sqlplus and a temp file to execute PL/SQL code inside a container
   */
  @Override
  public Observable<ConsoleOutput> execSqlFile(final String containerId, final String path) {
    if (path == null) {
      return Observable.empty();
    }
    LOGGER.debug("Executing SQL in {}", path);
    return dockerService.exec(containerId,
        "sqlplus", "-s",
        String.format(Locale.ROOT, "%s/%s@//0.0.0.0:1521/ORCLPDB1", config.getString("database.user"),
            config.getString("database.password")),
        String.format(Locale.ROOT, "@/%s", path)
    );
  }

  /**
   * {@inheritDoc} Not implemented, sqlplus cannot execute SQL on the command line
   */
  @Override
  public Observable<ConsoleOutput> execSql(final String containerId, final String sql) {
      return Observable.error(new Throwable("Not implemented"));
  }

  @Override
  public String getImage() {
    return config.getStringOrThrow("oracle.image");
  }

}
