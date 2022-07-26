package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.database.Flavor;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.functions.DockerFns;
import com.github.susom.database.DatabaseProvider;

/**
 * Factory methods for retrieving implementations for each database vendor
 */
public class FnFactory {

  public static AvroFns getAvroFns(Flavor flav, Job job, DatabaseProvider.Builder dbb) {
    switch (flav) {
      case sqlserver:
        return new SqlServerAvroFns(job, dbb);
      case oracle:
        return new OracleAvroFns(job, dbb);
      default:
        throw new RuntimeException("Flavor " + flav + " is not supported");
    }
  }

  public static DatabaseFns getDatabaseFns(Flavor flav, Config config, DatabaseProvider.Builder dbb) {
    switch (flav) {
      case sqlserver:
        return new SqlServerDatabaseFns(config, dbb);
      case oracle:
        return new OracleDatabaseFns(config, dbb);
      default:
        throw new RuntimeException("Flavor " + flav + " is not supported");
    }
  }

  public static DockerFns getDockerFns(Flavor flav, Config config) {
    switch (flav) {
      case sqlserver:
        return new SqlServerDockerFns(config);
      case oracle:
        return new OracleDockerFns(config);
      default:
        throw new RuntimeException("Flavor " + flav + " is not supported");
    }
  }

}
