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

import com.github.susom.starr.dbtoavro.jobrunner.entity.Table;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import com.github.susom.database.Config;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sql-server specific SQL statements, for various database tasks
 */
// TODO: Extract interface
public class SqlServerFns implements DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerFns.class);

  DatabaseProviderRx.Builder dbb;

  public SqlServerFns(Config config) {
    dbb = DatabaseProviderRx
        .pooledBuilder(config)
        .withSqlInExceptionMessages()
        .withSqlParameterLogging();
  }

  @Override
  public Completable transact(String sql) {
    return dbb.transactRx(db -> {
      db.get().ddl(sql).execute();
    });
  }

  @Override
  public Maybe<List<Table>> getTables(String schema) {
    return dbb.transactRx(db -> {
          db.get().ddl(String.format(Locale.CANADA, "USE %s", schema)).execute();
          List<Table> tableList = new ArrayList<>();
          db.get().toSelect(String.format(Locale.CANADA, 
              "SELECT DISTINCT table_name, DDPS.row_count\n"
                  + "FROM information_schema.tables\n"
                  + "         INNER JOIN SYS.OBJECTS AS OBJECTS ON OBJECTS.NAME = TABLES.TABLE_NAME\n"
                  + "         INNER JOIN SYS.DM_DB_PARTITION_STATS AS DDPS ON OBJECTS.OBJECT_ID = DDPS.OBJECT_ID\n"
                  + "WHERE TABLE_CATALOG = '%s'\n"
                  + "  AND TABLES.TABLE_TYPE = 'BASE TABLE'\n",
              schema))
              .queryMany(rs -> tableList.add(new Table(rs.getStringOrNull(1), rs.getLongOrZero(2))));
          return tableList;
        }
    );
  }

  public Single<String> getRestoreSql(String database, List<String> backupFiles) {
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
              StringBuilder sql = new StringBuilder("RESTORE DATABASE ").append(database).append(" FROM ")
                  .append("DISK=N'/backup/")
                  .append(String.join("', DISK=N'/backup/", backupFiles))
                  .append("' WITH ");
              for (int i = 0; i < files.size(); i++) {
                String logicalName = files.get(i).split("\\|")[0];
                String fileName = files.get(i).split("\\|")[1];
                String file = fileName.substring(fileName.lastIndexOf('\\') + 1);
                sql.append("MOVE N'").append(logicalName).append("' TO N'/var/opt/mssql/data/").append(file)
                    .append("', ");
              }
              sql.append("FILE=1, REPLACE, STATS=1");
              return Single.just(sql.toString());
            });
  }

}
