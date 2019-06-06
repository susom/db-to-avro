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

package com.github.susom.starr.dbtoavro.jobrunner.functions;

import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface DatabaseFns {

  /**
   * Run arbitrary SQL without a return value
   *
   * @param sql SQL code
   * @return Success of transaction
   */
  Completable transact(String sql);

  /**
   * Retrieve catalog, schema, table, and row-level information from a database
   *
   * @param containerId running database
   * @return database object
   */
  Single<Database> getDatabase(String containerId);

  /**
   * Get catalogs in database
   * @param database database to query
   * @return observable of catalogs
   */
  Observable<String> getCatalogs(Database database);

  Observable<String> getSchemas(String catalog);

  Observable<Table> getTables(String catalog, String schema);

  Single<Table> introspect(Table table);

  }
