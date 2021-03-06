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

package com.github.susom.starr.dbtoavro.functions;

import com.github.susom.database.Config;
import com.github.susom.database.DatabaseException;
import com.github.susom.starr.dbtoavro.entity.Database;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.SplitTableStrategy;
import com.github.susom.database.DatabaseProvider;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.io.File;
import java.sql.Types;
import java.util.List;
import java.util.Scanner;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatabaseFns {

  protected static final int[] supportedTypes = {
    Types.BIGINT,
    Types.BINARY,
    Types.BIT,
    Types.BLOB,
    Types.CHAR,
    Types.CLOB,
    Types.DATE,
    Types.DECIMAL,
    Types.DOUBLE,
    Types.FLOAT,
    Types.INTEGER,
    Types.LONGNVARCHAR,
    Types.LONGVARBINARY,
    Types.LONGVARCHAR,
    Types.NCHAR,
    Types.NCLOB,
    Types.NUMERIC,
    Types.NVARCHAR,
    Types.REAL,
    Types.SMALLINT,
    Types.TIMESTAMP,
    Types.TINYINT,
    Types.VARBINARY,
    Types.VARCHAR
  };
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseFns.class);
  protected final Config config;
  protected final DatabaseProvider.Builder dbb;

  public DatabaseFns(Config config, DatabaseProvider.Builder dbb) {
    this.config = config;
    this.dbb = dbb;
  }

  /**
   * Pointer to a database running in a docker container
   *
   * @param containerId running database
   * @return database object
   */
  public Single<Database> getDatabase(String containerId) {
    return Single.fromCallable(
        () ->
            dbb.transactReturning(
                db -> {
                  Database database = new Database(containerId);
                  database.flavor = db.get().flavor();
                  return database;
                }));
  }

  /**
   * Attempts to isValid to the DB
   *
   * @return Single boolean indicating if database connection was successful
   */
  public Single<Boolean> isValid() {
    return Single.fromCallable(
        () ->
            dbb.transactReturning(
                db -> {
                  LOGGER.debug("Attempting to connect to database...");
                  try {
                    return db.get().underlyingConnection().isValid(30000);
                  } catch (DatabaseException ex) {
                    LOGGER.error("Failed to connect: ", ex);
                    return false;
                  }
                }));
  }

  /**
   * Get schemas in catalog
   *
   * @param catalog catalog to query
   * @return observable of schemas
   */
  public abstract Observable<String> getSchemas(String catalog);

  /**
   * Get tables in a schema
   *
   * @param catalog catalog to query
   * @param schema schema to query
   * @param splitTables these tables use queries with where clause (oly applicable in Oracle)
   * @param priorities these tables are dumped first
   * @return observable of catalogs
   */
  public abstract Observable<String> getTables(String schema, Job job);

  /**
   * Prepare queries for export for a given table
   *
   * @param catalog catalog to query
   * @param schema schema to query
   * @param tableName table name
   * @param columnExclusions column exclusions
   * @param job job
   * @return observable of queries
   */
  public abstract Observable<Query> getQueries(String schema, String tableName, Job job);

  /**
   * Introspects a database table, required for selecting the appropriate splitting and exporting
   * method.
   *
   * @param catalog catalog to query
   * @param schema schema to query
   * @param table table to introspect
   * @param columnExclusions excludes any regexes that match against schema.table.column
   * @return Single of table with row counts, byte sizes, and supported column information
   */
  // public abstract Single<Query> introspect(String catalog, String schema, String table,
  // List<String> columnExclusions, String query);

  public abstract Single<String> getRestoreSql(String catalog, List<String> backupFiles);

  /**
   * Executes contents of an SQL file
   *
   * @param file path to SQL file
   * @return completable status
   */
  public Completable transactFile(File file) {
    if (file == null) {
      return Completable.complete();
    }
    return Completable.fromRunnable(
        () ->
            dbb.transact(
                db -> {
                  Scanner scanner = new Scanner(file, "UTF-8").useDelimiter(";");
                  while (scanner.hasNext()) {
                    String statement = scanner.next() + ";";
                    LOGGER.debug("Pre-SQL: {}", statement);
                    db.get().ddl(statement).execute();
                  }
                }));
  }

  /**
   * Executes an SQL string
   *
   * @return completable status
   */
  public Completable transact(String sql) {
    return Completable.fromRunnable(
        () -> {
          if (sql == null) {
            return;
          }
          dbb.transact(db -> db.get().ddl(sql).execute());
        });
  }

  protected boolean isSupported(int jdbcType) {
    return IntStream.of(supportedTypes).anyMatch(x -> x == jdbcType);
  }
}
