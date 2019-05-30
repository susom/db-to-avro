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
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse.Catalog;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse.Catalog.Schema;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Warehouse.Catalog.Schema.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql-server specific SQL statements, for various database tasks
 */
public class SqlServerDatabaseFns implements DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDatabaseFns.class);

  DatabaseProviderRx.Builder dbb;

  public SqlServerDatabaseFns(Config config) {
    dbb = DatabaseProviderRx
        .pooledBuilder(config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();
  }

  @Override
  public Completable transact(String sql) {
    if (sql == null) return Completable.complete();
    return dbb.transactRx(db -> {
      db.get().ddl(sql).execute();
    });
  }

  @Override
  public Single<Warehouse> getDatabase(String containerId) {

    // TODO: This can be parallelized!
    // TODO: Needs to log output, since this can take a LONG time
    // TODO: Remove getting table sizes? it's not used, really.

    return dbb.withConnectionAccess().transactRx(db -> {
      Warehouse database = new Warehouse(containerId);
      database.flavor = db.get().flavor();
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      // Retrieve catalogs
      ResultSet catalogs = metadata.getCatalogs();
      while (catalogs.next()) {
        String catalogName = catalogs.getString(1);
        if (catalogName.equals("master") || catalogName.equals("msdb") || catalogName.equals("tempdb")) {
          continue;
        }
        Warehouse.Catalog catalog = database.new Catalog(catalogName);
        // Retrieve schemas
        ResultSet schemas = metadata.getSchemas(catalogName, null);
        while (schemas.next()) {
          String schemaName = schemas.getString(1);
          Warehouse.Catalog.Schema schema = catalog.new Schema(schemaName);
          if (schemaName.equals("sys") || schemaName.startsWith("db_") || schemaName.equals("guest") || schemaName
              .equals("INFORMATION_SCHEMA")) {
            continue;
          }
          // Retrieve tables and their column information
          ResultSet tables = metadata.getTables(catalogName, schemaName, null, new String[]{"TABLE"});
          while (tables.next()) {
            String tableName = tables.getString(3);
            Warehouse.Catalog.Schema.Table table = schema.new Table(tableName);
            ResultSet columns = metadata.getColumns(catalogName, schemaName, tableName, "%");
            while (columns.next()) {
              String name = columns.getString(4);
              int type = columns.getInt(5);
              table.columns.add(table.new Column(name, type));
            }
            schema.tables.add(table);
          }
          catalog.schemas.add(schema);
        }
        database.catalogs.add(catalog);
      }

      // Get row counts and table sizes
      for (Catalog catalog : database.catalogs) {
        db.get().underlyingConnection().setCatalog(catalog.name);
        for (Schema schema : catalog.schemas) {
          db.get().underlyingConnection().setSchema(schema.name);
          // Table for storing table size information from stored procedure
          db.get().ddl("DROP TABLE IF EXISTS master.dbo.table_sizes").execute();
          db.get().ddl("CREATE TABLE master.dbo.table_sizes (\n"
              + "    name nvarchar(128),\n"
              + "    rows char(20),\n"
              + "    reserved varchar(18),\n"
              + "    data varchar(18),\n"
              + "    index_size varchar(18),\n"
              + "    unused varchar(18)\n"
              + ")").execute();
          for (Table table : schema.tables) {
            db.get().ddl(
                "INSERT INTO master.dbo.table_sizes EXEC ('sp_spaceused N''" + schema.name + "." + table.name + "''')")
                .execute();
            table.bytes =
                1024 * Long.parseLong(db.get().toSelect("SELECT data FROM master.dbo.table_sizes WHERE name = ?")
                    .argString(table.name)
                    .queryStringOrEmpty().replace(" KB", ""));
            table.rows = db.get().toSelect("SELECT SUM(PARTITIONS.rows) AS rows\n"
                + "FROM sys.objects OBJECTS\n"
                + "         INNER JOIN sys.partitions PARTITIONS ON OBJECTS.object_id = PARTITIONS.object_id\n"
                + "WHERE OBJECTS.type = 'U'\n"
                + "  AND PARTITIONS.index_id < 2\n"
                + "  AND SCHEMA_NAME(OBJECTS.schema_id) = ?\n"
                + "  AND OBJECTS.Name = ?")
                .argString(schema.name)
                .argString(table.name)
                .queryLongOrZero();
          }
        }
      }
      return database;
    }).toSingle();
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

}
