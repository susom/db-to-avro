package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.docker.ConsoleOutput;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import io.reactivex.Completable;
import io.reactivex.Observable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        String.format("userid=%s/%s@//0.0.0.0:1521/ORCLPDB1", config.getString("database.user"),
            config.getString("database.password")),
        (impdpThreads > 0) ? "PARALLEL=" + impdpThreads : "",
        "PARFILE=/backup/"+backupFiles.get(0)
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

    // TODO: make more robust. Need to check for strings
    // "ORA-12514" (DB not booted)
    // "ORA-28000" (account is locked) etc.

    List<String> stdout = new ArrayList<>();
    return execSqlShell(containerId, "SELECT 1 FROM DUAL;")
        .doOnNext(o -> stdout.add(o.getData()))
        .filter(p -> p.getData().contains("----------"))
        .count()
        .flatMapCompletable(count -> {
          if (count > 0) {
            // Database may not actually be 100% done yet, tablespace modifications will fail with ORA-01155
            Thread.sleep(15000);
            return Completable.complete();
          } else {
            return Completable.error(new Throwable("Failed to connect to database" + String.join("\n", stdout)));
          }
        });
  }

  @Override
  public String getImage() {
    return config.getStringOrThrow("oracle.image");
  }

}
