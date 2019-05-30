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

package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.SqlServerDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Load;
import com.github.susom.starr.dbtoavro.jobrunner.runner.JobLogger;
import io.reactivex.Single;

/**
 * Loads an already existing SQL server instance
 */
public class SqlServerLoadExisting extends Load {

  private SqlServerDatabaseFns db;

  public SqlServerLoadExisting(Config config) {
    super(config);
    this.db = new SqlServerDatabaseFns(config);
  }

  @Override
  public Single<Warehouse> run(Job job, JobLogger logger) {
    return
        db.transact(job.postSql)
            .doOnComplete(() -> logger.log("Database post-sql completed, introspecting database"))
            .andThen(db.getDatabase(null))
            .doFinally(() -> logger.log("Database introspection complete"));
  }

}
