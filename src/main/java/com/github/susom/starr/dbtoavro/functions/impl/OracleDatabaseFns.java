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

package com.github.susom.starr.dbtoavro.functions.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.entity.Column;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx.Builder;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql-server specific SQL statements, for various database tasks
 */
public class OracleDatabaseFns extends DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseFns.class);

  public OracleDatabaseFns(Config config, Builder dbb) {
    super(config, dbb);
  }

  @Override
  public Observable<String> getSchemas(String catalog) {
    return dbb.transactRx(db -> {
      LOGGER.debug("Enumerating schemas...");
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
  public Single<Table> introspect(String catalog, String schema, String table,
    List<String> columnExclusions) {
    return dbb.transactRx(db -> {
      LOGGER.debug("Introspecting {}", table);

      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();

      // Retrieve columns
      List<Column> cols = new ArrayList<>();
      try (ResultSet columns = metadata.getColumns(catalog, schema, table, "%")) {
        while (columns.next()) {
          String name = columns.getString(4);
          int jdbcType = columns.getInt(5);
          String vendorType = columns.getString(6);
          boolean supported = isSupported(jdbcType);
          boolean exclude = columnExclusions.stream()
            .anyMatch(re -> (schema + "." + table + "." + name).matches("(?i:" + re + ")"));
          cols.add(new Column(name, jdbcType, vendorType, supported, exclude));
          if (!supported) {
            LOGGER
              .debug("[{}].[{}].[{}] has unsupported type {} ({})",
                schema, table, name, vendorType, jdbcType);
          }
        }
        // Get primary keys
        try (ResultSet pks = metadata.getPrimaryKeys(catalog, schema, table)) {
          while (pks.next()) {
            String colName = pks.getString(4);
            cols.stream().filter(c -> c.name.equals(colName)).forEach(c -> c.primaryKey = true);
          }
        }
      }

      return new Table(catalog, schema, table, cols);

    }).toSingle();
  }

  /**
   * Not used in Oracle
   */
  @Override
  public Single<String> getRestoreSql(String catalog, List<String> backupFiles) {
    return null;
  }

  @Override
  public Observable<String> getTables(String catalog, String schema, List<String> priorities) {
    return dbb.transactRx(db -> {

      List<String> schemaTables = priorities.stream()
        .filter(t -> t.startsWith(schema + "."))
        .map(t -> t.split("\\.")[1])
        .collect(Collectors.toList());

      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet tables = metadata.getTables(catalog, schema, null, new String[]{"TABLE"})) {
        List<String> tablesList = new ArrayList<>(schemaTables);
        while (tables.next()) {
          String tableName = tables.getString(3);
          if (!tablesList.contains(tableName)) {
            tablesList.add(tableName);
          }
        }
        return tablesList;
      }
    }).toObservable().flatMapIterable(l -> l);
  }

}
