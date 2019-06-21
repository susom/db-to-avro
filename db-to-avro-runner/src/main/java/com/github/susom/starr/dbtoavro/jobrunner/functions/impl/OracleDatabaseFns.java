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

package com.github.susom.starr.dbtoavro.jobrunner.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Column;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Database;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx.Builder;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql-server specific SQL statements, for various database tasks
 */
public class OracleDatabaseFns extends DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseFns.class);
  private static int[] serializable = {
      Types.BIGINT,
      Types.BINARY,
      Types.BLOB,
      Types.CHAR,
      Types.CLOB,
      Types.DOUBLE,
      Types.DECIMAL,
      Types.FLOAT,
      Types.INTEGER,
      Types.NCHAR,
      Types.NCLOB,
      Types.NUMERIC,
      Types.NVARCHAR,
      Types.REAL,
      Types.SMALLINT,
      Types.TINYINT,
      Types.TIMESTAMP,
      Types.VARBINARY,
      Types.VARCHAR
  };

  public OracleDatabaseFns(Config config, Builder dbb) {
    super(config, dbb);
  }

  @Override
  public Observable<String> getSchemas(String catalog) {
    return dbb.withConnectionAccess().transactRx(db -> {
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet schemas = metadata.getSchemas(null, null)) {
        List<String> schemasList = new ArrayList<>();
        while (schemas.next()) {
          schemasList.add(schemas.getString(1));
        }
        return schemasList;
      }
    }).toObservable().flatMapIterable(l -> l);
  }

  @Override
  public Observable<Table> introspect(String catalog, String schema, String table) {
    return dbb.withConnectionAccess().transactRx(db -> {
      LOGGER.info("Introspecting table {}", table);
      db.get().underlyingConnection().setSchema(schema);

      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();

      // Retrieve columns
      List<Column> cols = new ArrayList<>();
      try (ResultSet columns = metadata.getColumns(catalog, schema, table, "%")) {
        while (columns.next()) {
          String colName = columns.getString(4);
          int type = columns.getInt(5);
          String typeName = columns.getString(6);
          cols.add(new Column(colName, type, typeName, isSerializable(type)));
          LOGGER.debug("Table {} Column {} Type {} ({}) Supported {}", table, colName, typeName, type, isSerializable(type));
        }
        // Get primary keys
        try (ResultSet pks = metadata.getPrimaryKeys(catalog, schema, table)) {
          while (pks.next()) {
            String colName = pks.getString(4);
            cols.stream().filter(c -> c.name.equals(colName)).forEach(c -> c.primaryKey = true);
          }
        }
      }

      // Number of bytes
      long bytes = db.get().toSelect("SELECT BYTES FROM DBA_SEGMENTS WHERE SEGMENT_TYPE='TABLE' AND SEGMENT_NAME = ?")
          .argString(table)
          .queryLongOrZero();

      // Oracle defaults to an insane amount of threads killing the app
      int cores = Runtime.getRuntime().availableProcessors();

      // Number of rows
//      String sql = String.format(Locale.CANADA, "SELECT /*+ FULL(%1$s) PARALLEL(%1$s, %2$d) */ COUNT(*) FROM %1$s", table, cores);
//      long rows = db.get().toSelect(sql).queryLongOrZero();

      return new Table(catalog, schema, table, cols, bytes, 0);

    }).toObservable();
  }

  /**
   * Not used in Oracle
   */
  @Override
  public Single<String> getRestoreSql(String catalog, List<String> backupFiles) {
    return null;
  }

  @Override
  public Observable<String> getTables(String catalog, String schema) {
    return dbb.withConnectionAccess().transactRx(db -> {
      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet tables = metadata.getTables(catalog, schema, null, new String[]{"TABLE"})) {
        List<String> tablesList = new ArrayList<>();
        int counter = 0;
        while (tables.next()) {
          tablesList.add(tables.getString(3));
          if (++counter % 100 == 0) {
            LOGGER.debug("{} tables read...", counter);
          }
        }
        LOGGER.debug("{} tables read...", counter);
        return tablesList;
      }
    }).toObservable().flatMapIterable(l -> l);
  }

  @Override
  public Single<Database> getDatabase(String containerId) {
    return dbb.withConnectionAccess().transactRx(db -> {
      Database database = new Database(containerId);
      database.flavor = db.get().flavor();
      return database;
    }).toSingle();
  }

  private boolean isSerializable(int type) {
    return IntStream.of(serializable).anyMatch(x -> x == type);
  }

}
