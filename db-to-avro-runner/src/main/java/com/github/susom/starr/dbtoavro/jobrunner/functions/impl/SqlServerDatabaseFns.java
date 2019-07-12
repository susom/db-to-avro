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
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql-server specific SQL statements, for various database tasks
 */
public class SqlServerDatabaseFns extends DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDatabaseFns.class);

  private static int[] serializable = {
      Types.BIGINT,
      Types.BINARY,
      Types.BLOB,
      Types.CHAR,
      Types.CLOB,
      Types.DOUBLE,
      Types.INTEGER,

    // TODO: Are these used in MSSQL?
//      Types.FLOAT,
//      Types.DECIMAL,

      Types.NCHAR,
      Types.NCLOB,
      Types.NUMERIC,
      Types.NVARCHAR,
      Types.REAL,
      Types.SMALLINT,
      Types.TINYINT,
      Types.TIMESTAMP,
//      Types.VARBINARY, // This is SpatialLocation in MSSQL
      Types.VARCHAR
  };

  public SqlServerDatabaseFns(Config config, Builder dbb) {
    super(config, dbb);
  }

  @Override
  public Completable transact(String sql) {
    if (sql == null) {
      return Completable.complete();
    }
    return dbb.transactRx(db -> {
      db.get().ddl(sql).execute();
    });
  }

  @Override
  public Observable<String> getSchemas(String catalog) {
    return dbb.withConnectionAccess().transactRx(db -> {
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet schemas = metadata.getSchemas(catalog, null)) {
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
    return dbb.withConnectionAccess().transactRx(db -> {
      LOGGER.info("Introspecting table {}", table);
      db.get().underlyingConnection().setCatalog(catalog);
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
        }
        // Get primary keys
        try (ResultSet pks = metadata.getPrimaryKeys(catalog, schema, table)) {
          while (pks.next()) {
            String colName = pks.getString(4);
            cols.stream().filter(c -> c.name.equals(colName)).forEach(c -> c.primaryKey = true);
          }
        }
      }

      // Size of table in bytes
      long bytes = 0;
      try (CallableStatement spaceUsed = db.get().underlyingConnection()
          .prepareCall("{call sp_spaceused(?)}")) {
        spaceUsed.setString(1, schema + "." + table);
        if (spaceUsed.execute()) {
          try (ResultSet rs = spaceUsed.getResultSet()) {
            while (rs.next()) {
              bytes = Long.valueOf(rs.getString(4).replace(" KB", "")) * 1000;
            }
          }
        }
      } catch (SQLException expected) {
      }

      // Number of rows
      long rows = db.get().toSelect("SELECT SUM(PARTITIONS.rows) AS rows\n"
          + "FROM sys.objects OBJECTS\n"
          + "         INNER JOIN sys.partitions PARTITIONS ON OBJECTS.object_id = PARTITIONS.object_id\n"
          + "WHERE OBJECTS.type = 'U'\n"
          + "  AND PARTITIONS.index_id < 2\n"
          + "  AND SCHEMA_NAME(OBJECTS.schema_id) = ?\n"
          + "  AND OBJECTS.Name = ?")
          .argString(schema)
          .argString(table)
          .queryLongOrZero();

      return new Table(catalog, schema, table, cols, bytes, rows);

    }).toObservable();
  }

  @Override
  public Observable<String> getTables(String catalog, String schema) {
    return dbb.withConnectionAccess().transactRx(db -> {
      db.get().underlyingConnection().setCatalog(catalog);
      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet tables = metadata.getTables(catalog, schema, null, new String[]{"TABLE"})) {
        List<String> tablesList = new ArrayList<>();
        while (tables.next()) {
          tablesList.add(tables.getString(3));
        }
        return tablesList;
      }
    }).toObservable().flatMapIterable(l -> l);
  }

  /**
   * SQL-server specific function to create the database restore ddl
   *
   * @param catalog catalog to restore
   * @param backupFiles list of backup files in restore
   * @return SQL that when executed starts a restore
   */
  public Single<String> getRestoreSql(String catalog, List<String> backupFiles) {
    return
        dbb.transactRx(db -> {
          db.get().ddl("DROP TABLE IF EXISTS FileListHeaders").execute();
          db.get().ddl("CREATE TABLE FileListHeaders (\n"
              + "     LogicalName    nvarchar(128)\n"
              + "    ,PhysicalName   nvarchar(260)\n"
              + "    ,[Type] char(1)\n"
              + "    ,FileGroupName  nvarchar(128) NULL\n"
              + "    ,Size   numeric(20,0)\n"
              + "    ,MaxSize    numeric(20,0)\n"
              + "    ,FileID bigint\n"
              + "    ,CreateLSN  numeric(25,0)\n"
              + "    ,DropLSN    numeric(25,0) NULL\n"
              + "    ,UniqueID   uniqueidentifier\n"
              + "    ,ReadOnlyLSN    numeric(25,0) NULL\n"
              + "    ,ReadWriteLSN   numeric(25,0) NULL\n"
              + "    ,BackupSizeInBytes  bigint\n"
              + "    ,SourceBlockSize    int\n"
              + "    ,FileGroupID    int\n"
              + "    ,LogGroupGUID   uniqueidentifier NULL\n"
              + "    ,DifferentialBaseLSN    numeric(25,0) NULL\n"
              + "    ,DifferentialBaseGUID   uniqueidentifier NULL\n"
              + "    ,IsReadOnly bit\n"
              + "    ,IsPresent  bit\n"
              + ")").execute();
          db.get().ddl(
              "IF cast(cast(SERVERPROPERTY('ProductVersion') as char(4)) as float) > 9 -- Greater than SQL 2005\n"
                  + "BEGIN\n"
                  + "    ALTER TABLE FileListHeaders ADD TDEThumbprint varbinary(32) NULL\n"
                  + "END").execute();
          db.get().ddl(
              "IF cast(cast(SERVERPROPERTY('ProductVersion') as char(2)) as float) > 12 -- Greater than 2014\n"
                  + "BEGIN\n"
                  + "    ALTER TABLE FileListHeaders ADD SnapshotURL nvarchar(360) NULL\n"
                  + "END").execute();
          StringBuilder sql = new StringBuilder(
              "INSERT INTO FileListHeaders EXEC ('RESTORE FILELISTONLY FROM DISK=N''/backup/");
          sql.append(String.join("'', DISK=N''/backup/", backupFiles)).append("''')");
          db.get().ddl(sql.toString()).execute();
        })
            .andThen(
                dbb.transactRx(db -> {
                      return db.get().toSelect("SELECT LogicalName, PhysicalName FROM FileListHeaders")
                          .queryMany(rs -> rs.getStringOrNull(1) + "|" + rs.getStringOrNull(2));
                    }
                ))
            .flatMapSingle(files -> {
              StringBuilder sql = new StringBuilder("RESTORE DATABASE ").append(catalog).append(" FROM ")
                  .append("DISK=N'/backup/")
                  .append(String.join("', DISK=N'/backup/", backupFiles))
                  .append("' WITH ");
              for (String logicalFile : files) {
                String logicalName = logicalFile.split("\\|")[0];
                String fileName = logicalFile.split("\\|")[1];
                String file = fileName.substring(fileName.lastIndexOf('\\') + 1);
                sql.append("MOVE N'").append(logicalName).append("' TO N'/var/opt/mssql/data/").append(file)
                    .append("', ");
              }
              sql.append("FILE=1, REPLACE, STATS=1");
              return Single.just(sql.toString());
            });
  }

  private boolean isSerializable(int type) {
    return IntStream.of(serializable).anyMatch(x -> x == type);
  }

}
