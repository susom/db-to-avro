package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.database.Flavor;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DockerFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;

public class FnFactory {

  public static AvroFns getAvroFns(Flavor flav, Config config, DatabaseProviderRx.Builder dbb)  {
    switch (flav) {
      case sqlserver:
        return new SqlServerAvroFns(config, dbb);
      default:
        return null;
    }
  }

  public static DatabaseFns getDatabaseFns(Flavor flav, Config config, DatabaseProviderRx.Builder dbb)  {
    switch (flav) {
      case sqlserver:
        return new SqlServerDatabaseFns(config, dbb);
      default:
        return null;
    }
  }

  public static DockerFns getDockerFns(Flavor flav, Config config)  {
    switch (flav) {
      case sqlserver:
        return new SqlServerDockerFns(config);
      default:
        return null;
    }
  }

}
