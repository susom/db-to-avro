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

package com.github.susom.starr.db_to_avro.jobrunner.functions;

import com.github.susom.starr.db_to_avro.jobrunner.entity.Table;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import java.util.List;

public interface DatabaseFns {

  /**
   * Run arbitrary SQL without a return value
   * @param sql SQL code
   * @return Success of transaction
   */
  Completable transact(String sql);

  /**
   * Get a list of tables and row counts within a given schema/catalog
   * @param schema schema or catalog
   * @return Maybe a list of {@link Table}
   */
  Maybe<List<Table>> getTables(String schema);

  /**
   * For databases that initiate restore via SQL
   * @param database database to restore
   * @param backupFiles files containing the backups
   * @return Vendor-specific SQL code for restoring a backup
   */
  Single<String> getRestoreSql(String database, List<String> backupFiles);
}
