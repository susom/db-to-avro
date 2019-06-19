package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import io.reactivex.Completable;
import io.reactivex.Observable;
import java.util.Arrays;
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

  public Observable<ConsoleOutput> impdp(final String containerId, final String parfile) {
    return dockerService.exec(containerId,
        "impdp",
        String.format("userid=%s/%s@//0.0.0.0:1521/ORCLPDB1", config.getString("database.user"),
            config.getString("database.password")),
        (impdpThreads > 0)?"PARALLEL="+impdpThreads:"",
        "PARFILE=/backup/"+parfile
    );
  }

  /**
   * {@inheritDoc} Uses sqlplus and a temp file to execute PL/SQL code
   */
  @Override
  public Observable<ConsoleOutput> execSqlShell(final String containerId, final String query) {
    String filename = UUID.randomUUID().toString() + ".sql";
    dockerService.createFileFromString(containerId, filename, query);
    return dockerService.exec(containerId,
        "sqlplus", "-s",
        String.format("%s/%s@//0.0.0.0:1521/ORCLPDB1", config.getString("database.user"),
            config.getString("database.password")),
        String.format("@/%s", filename)
    );
  }

  @Override
  public Completable healthCheck(final String containerId) {
    return execSqlShell(containerId, "SELECT 1 FROM DUAL;")
        .filter(p -> p.getData().contains("----------"))
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
    return config.getStringOrThrow("oracle.image");
  }

}
