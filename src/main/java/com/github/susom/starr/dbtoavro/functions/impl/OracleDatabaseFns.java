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
import com.github.susom.starr.dbtoavro.entity.Database;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx.Builder;
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
    LOGGER.debug("Enumerating schemas...");
    return dbb.transactRx(db -> {
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
  public Observable<Table> introspect(String catalog, String schema, String table, List<String> filters) {
    return dbb.transactRx(db -> {
      db.get().underlyingConnection().setSchema(schema);

      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();

      // Retrieve columns
      List<Column> cols = new ArrayList<>();
      try (ResultSet columns = metadata.getColumns(catalog, schema, table, "%")) {
        while (columns.next()) {
          String colName = columns.getString(4);
          int type = columns.getInt(5);
          String typeName = columns.getString(6);
          boolean serializable = isSerializable(type)
              && filters.stream().noneMatch(s -> s.equals(schema + "." + table + "." + colName));
          cols.add(new Column(colName, type, typeName, serializable));
          if (!isSerializable(type)) {
            LOGGER.debug("Table {} Column {} Type {} ({}) will be ignored", table, colName, typeName, type);
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

      // Number of bytes
      long bytes = db.get().toSelect("\n"
          + "SELECT SUM(BYTES)\n"
          + "FROM (SELECT SEGMENT_NAME TABLE_NAME, OWNER, BYTES\n"
          + "      FROM DBA_SEGMENTS\n"
          + "      WHERE SEGMENT_TYPE IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')\n"
          + "      UNION ALL\n"
          + "      SELECT I.TABLE_NAME, I.OWNER, S.BYTES\n"
          + "      FROM DBA_INDEXES I,\n"
          + "           DBA_SEGMENTS S\n"
          + "      WHERE S.SEGMENT_NAME = I.INDEX_NAME\n"
          + "        AND S.OWNER = I.OWNER\n"
          + "        AND S.SEGMENT_TYPE IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION')\n"
          + "      UNION ALL\n"
          + "      SELECT L.TABLE_NAME, L.OWNER, S.BYTES\n"
          + "      FROM DBA_LOBS L,\n"
          + "           DBA_SEGMENTS S\n"
          + "      WHERE S.SEGMENT_NAME = L.SEGMENT_NAME\n"
          + "        AND S.OWNER = L.OWNER\n"
          + "        AND S.SEGMENT_TYPE IN ('LOBSEGMENT', 'LOB PARTITION')\n"
          + "      UNION ALL\n"
          + "      SELECT L.TABLE_NAME, L.OWNER, S.BYTES\n"
          + "      FROM DBA_LOBS L,\n"
          + "           DBA_SEGMENTS S\n"
          + "      WHERE S.SEGMENT_NAME = L.INDEX_NAME\n"
          + "        AND S.OWNER = L.OWNER\n"
          + "        AND S.SEGMENT_TYPE = 'LOBINDEX')\n"
          + "WHERE OWNER = ?\n"
          + "  AND TABLE_NAME = ?\n")
          .argString(schema)
          .argString(table)
          .queryLongOrZero();

      // Number of rows
      String sql = String.format(Locale.CANADA, "SELECT COUNT(*) FROM %s", table);
      long rows = db.get().toSelect(sql).queryLongOrZero();

      return new Table(catalog, schema, table, cols, bytes, rows);

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
    return dbb.transactRx(db -> {
      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet tables = metadata.getTables(catalog, schema, null, new String[]{"TABLE"})) {
        List<String> tablesList = new ArrayList<>();
        while (tables.next()) {
          String name = tables.getString(3);
          if (!name.contains("SYS_IOT")) {
            tablesList.add(tables.getString(3));
          }
        }
        return tablesList;
      }
    }).toObservable().flatMapIterable(l -> l);
  }

  private boolean isSerializable(int type) {
    return IntStream.of(serializable).anyMatch(x -> x == type);
  }

}
