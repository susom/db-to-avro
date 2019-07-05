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
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.OracleDatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Completable;
import io.reactivex.Single;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads an (already) running SQL server instance
 */
public class OracleLoadDatabase implements Loader {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleLoadDatabase.class);

  private OracleDatabaseFns db;

  public OracleLoadDatabase(Config config, DatabaseProviderRx.Builder dbb) {
    this.db = new OracleDatabaseFns(config, dbb);
  }

  @Override
  public Single<Database> run(Job job) {
    LOGGER.info("Using existing Oracle server database");
    return
        db.transactFile(job.postSql)
            .doOnComplete(() -> LOGGER.info("Database post-sql completed"))
            .andThen(db.getDatabase(null));
  }

  @Override
  public Completable stop(Database database) {
    return Completable.complete();
  }

}
