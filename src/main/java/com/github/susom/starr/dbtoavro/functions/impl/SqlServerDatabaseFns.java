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
import com.github.susom.database.Database;
import com.github.susom.database.DatabaseProvider.Builder;
import com.github.susom.database.SqlSelect;
import com.github.susom.starr.dbtoavro.entity.Column;
import com.github.susom.starr.dbtoavro.entity.Statistics;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.KeyDataType;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.SplitTableStrategy;
import com.github.susom.starr.dbtoavro.entity.SplitTableStrategyOperation;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.Single;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jdk.internal.org.jline.reader.Completer;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sql-server specific SQL statements, for various database tasks */
public class SqlServerDatabaseFns extends DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerDatabaseFns.class);
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static int STRING_DATE_CONVERSION = 126;
  private static String SQL_DECORATOR = ""; // " WITH (NOLOCK) ";
  private static String SQL_DECORATOR_NON_DATA = ""; // " WITH (NOLOCK) ";
  private static String SQL_DECORATOR_NO_WHERE = " WITH (NOLOCK) ";

  public SqlServerDatabaseFns(Config config, Builder dbb) {
    super(config, dbb);
  }

  @Override
  public Completable transact(String sql) {
    if (sql == null) {
      return Completable.complete();
    }
    return Completable.fromRunnable(() -> dbb.transact(db -> db.get().ddl(sql).execute()));
  }

  @Override
  public Observable<String> getSchemas(String catalog) {
    return Observable.fromCallable(
            () ->
                dbb.transactReturning(
                    db -> {
                      LOGGER.debug("Enumerating schemas...");
                      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
                      try (ResultSet schemas = metadata.getSchemas(catalog, null)) {
                        List<String> schemasList = new ArrayList<>();
                        while (schemas.next()) {
                          schemasList.add(schemas.getString(1));
                        }
                        return schemasList;
                      }
                    }))
        .flatMapIterable(l -> l);
  }

  private List<Column> retrieveColumns(
      DatabaseMetaData metadata,
      String catalog,
      String schema,
      String table,
      List<String> columnExclusions)
      throws SQLException {
    // Retrieve columns
    List<Column> cols = new ArrayList<>();
    try (ResultSet columns = metadata.getColumns(catalog, schema, table, "%")) {
      while (columns.next()) {
        String name = columns.getString(4);
        int jdbcType = columns.getInt(5);
        String vendorType = columns.getString(6);
        boolean supported = isSupported(jdbcType);
        boolean exclude =
            columnExclusions.stream()
                .anyMatch(re -> (schema + "." + table + "." + name).matches("(?i:" + re + ")"));
        cols.add(new Column(name, jdbcType, vendorType, supported, exclude));
        if (!supported) {
          LOGGER.debug(
              "[{}].[{}].[{}] has unsupported type {} ({})",
              schema,
              table,
              name,
              vendorType,
              jdbcType);
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
    return cols;
  }

  /**
   * SQL-server specific function to create the database restore ddl
   *
   * @param catalog catalog to restore
   * @param backupFiles list of backup files in restore
   * @return SQL that when executed starts a restore
   */
  public Single<String> getRestoreSql(String catalog, List<String> backupFiles) {
    return Single.fromCallable(
        () ->
            dbb.transactReturning(
                db -> {
                  db.get().ddl("DROP TABLE IF EXISTS FileListHeaders").execute();
                  db.get()
                      .ddl(
                          "CREATE TABLE FileListHeaders (\n"
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
                              + ")")
                      .execute();

                  db.get()
                      .ddl(
                          "IF cast(cast(SERVERPROPERTY('ProductVersion') as char(4)) as float) > 9 -- Greater than SQL 2005\n"
                              + "BEGIN\n"
                              + "    ALTER TABLE FileListHeaders ADD TDEThumbprint varbinary(32) NULL\n"
                              + "END")
                      .execute();

                  db.get()
                      .ddl(
                          "IF cast(cast(SERVERPROPERTY('ProductVersion') as char(2)) as float) > 12 -- Greater than 2014\n"
                              + "BEGIN\n"
                              + "    ALTER TABLE FileListHeaders ADD SnapshotURL nvarchar(360) NULL\n"
                              + "END")
                      .execute();

                  db.get()
                      .ddl(
                          "INSERT INTO FileListHeaders EXEC ('RESTORE FILELISTONLY FROM DISK=N''/backup/"
                              + String.join("'', DISK=N''/backup/", backupFiles)
                              + "''')")
                      .execute();

                  List<String> files =
                      db.get()
                          .toSelect("SELECT LogicalName, PhysicalName FROM FileListHeaders")
                          .queryMany(rs -> rs.getStringOrNull(1) + "|" + rs.getStringOrNull(2));

                  StringBuilder sql =
                      new StringBuilder("RESTORE DATABASE ")
                          .append(catalog)
                          .append(" FROM ")
                          .append("DISK=N'/backup/")
                          .append(String.join("', DISK=N'/backup/", backupFiles))
                          .append("' WITH ");

                  for (String logicalFile : files) {
                    String logicalName = logicalFile.split("\\|")[0];
                    String fileName = logicalFile.split("\\|")[1];
                    String file = fileName.substring(fileName.lastIndexOf('\\') + 1);
                    sql.append("MOVE N'")
                        .append(logicalName)
                        .append("' TO N'/var/opt/mssql/data/")
                        .append(file)
                        .append("', ");
                  }
                  sql.append("FILE=1, REPLACE, STATS=1");
                  return sql.toString();
                }));
  }

  @Override
  public Observable<String> getTables(String schema, Job job) {
    return Observable.fromCallable(
            () -> {
              String catalog = job.catalog;
              List<String> priorities = job.tablePriorities;
              List<SplitTableStrategy> splitTableStrategy = job.splitTableStrategies;
              return dbb.transactReturning(
                  db -> {
                    List<String> prioritizeAndSplitTable =
                        priorities.stream()
                            .filter(t -> t.startsWith(schema + "."))
                            .map(t -> t.split("\\.")[1])
                            .collect(Collectors.toList());

                    if (null != splitTableStrategy) {
                      prioritizeAndSplitTable.addAll(
                          splitTableStrategy.stream()
                              .filter(
                                  x ->
                                      x.getTableName().contains(".")
                                          ? !prioritizeAndSplitTable.contains(
                                              x.getTableName().split("\\.")[1])
                                          : !prioritizeAndSplitTable.contains(x.getTableName()))
                              .map(
                                  x ->
                                      x.getTableName().contains(".")
                                          ? x.getTableName().split("\\.")[1]
                                          : x.getTableName())
                              .collect(Collectors.toList()));
                    }
                    // Removing duplicate tables from the list
                    List<String> prioritizeAndSplitTableLst =
                        prioritizeAndSplitTable.stream().distinct().collect(Collectors.toList());

                    LOGGER.info(
                        "Table List Prepared (Prioritized/Split): TableCount={}",
                        prioritizeAndSplitTableLst.size());

                    db.get().underlyingConnection().setSchema(schema);
                    DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
                    try (ResultSet tables =
                        metadata.getTables(catalog, schema, null, new String[] {"TABLE"})) {
                      List<String> tablesList = new ArrayList<>(prioritizeAndSplitTableLst);
                      while (tables.next()) {
                        String tableName = tables.getString(3);
                        if (!tablesList.contains(tableName)) {
                          tablesList.add(tableName);
                        }
                      }
                      LOGGER.info(
                          "Table List Prepared (All before filtering): TableCount={}",
                          tablesList.size());
                      return tablesList;
                    }
                  });
            })
        .flatMapIterable(l -> l);
  }

  @Override
  public Observable<Query> getQueries(String schema, String tableName, Job job) {
    return Observable.fromCallable(
            () -> {
//              LOGGER.warn(">>>>>>>>>>>>>> STARTING GET QUERIES <<<<<<<<<");
              String catalog = job.catalog;
              List<String> columnExclusions = job.columnExclusions;
              long incrementFactor = job.incrementFactor;
              return dbb.transactReturning(
                  db -> {
                    db.get().underlyingConnection().setSchema(schema);
                    DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();

                    List<Column> cols =
                        retrieveColumns(metadata, catalog, schema, tableName, columnExclusions);
                    if (cols.stream().noneMatch(Column::isExportable)) {
                      LOGGER.warn("Skipping table {}, no columns are exportable", tableName);
                      return new ArrayList<Query>();
                    }

                    List<SplitTableStrategy> splitTableStrategies =
                        job.splitTableStrategies.stream()
                            .filter(
                                x ->
                                    x.getTableName().contains(".")
                                        ? x.getTableName()
                                            .split("\\.")[1]
                                            .equalsIgnoreCase(tableName)
                                        : x.getTableName().equalsIgnoreCase(tableName))
                            .collect(Collectors.toList());
                    List<String> listQueries = new ArrayList<>();
                    Table table =
                        new Table(
                            catalog,
                            schema,
                            tableName,
                            cols,
                            getDbRowCount(db.get(), catalog, schema, tableName),
                            splitTableStrategies);
                    String columnSql = getColumnSql(job, table);

                    splitTableStrategies.forEach(
                        splitTableStrategy -> {
                          if (splitTableStrategy.getOperation() != SplitTableStrategyOperation.query
                              && (splitTableStrategy.getKeyDataType() == KeyDataType.date
                                  || splitTableStrategy.getKeyDataType() == KeyDataType.number)) {
                            if (splitTableStrategy.getStartRange() == null
                                || splitTableStrategy.getEndRange() == null) {
                              if (splitTableStrategy.getOperation()
                                  == SplitTableStrategyOperation.between) {
                                String sql =
                                    String.format(
                                        Locale.ROOT,
                                        "SELECT MIN(%s) as min_range, MAX(%s) as max_range FROM [%s].[%s].[%s] %s",
                                        splitTableStrategy.getColumn(),
                                        splitTableStrategy.getColumn(),
                                        catalog,
                                        schema,
                                        tableName,
                                        SQL_DECORATOR_NON_DATA);
                                SqlSelect sqlSelect = db.get().toSelect(sql);
                                sqlSelect.query(
                                    rs -> {
                                      while (rs.next()) {
                                        splitTableStrategy.setStartRange(rs.getLongOrNull());
                                        splitTableStrategy.setEndRange(rs.getLongOrNull());
                                      }
                                      return splitTableStrategy;
                                    });
                              } else if (splitTableStrategy.getOperation()
                                      == SplitTableStrategyOperation.range
                                  && splitTableStrategy.getKeyDataType() == KeyDataType.date) {
                                String sql =
                                    String.format(
                                        Locale.ROOT,
                                        "SELECT MIN(%s) as min_range, MAX(%s) as max_range FROM [%s].[%s].[%s] %s",
                                        splitTableStrategy.getColumn(),
                                        splitTableStrategy.getColumn(),
                                        catalog,
                                        schema,
                                        tableName,
                                        SQL_DECORATOR_NON_DATA);
                                SqlSelect sqlSelect = db.get().toSelect(sql);
                                sqlSelect.query(
                                    rs -> {
                                      while (rs.next()) {
                                        splitTableStrategy.setStartRangeDate(rs.getDateOrNull());
                                        splitTableStrategy.setEndRangeDate(rs.getDateOrNull());
                                      }
                                      return splitTableStrategy;
                                    });
                              }
                            }
                          }
                          LOGGER.info("splitTableStrategy = {}", splitTableStrategy);

                          if (splitTableStrategy.getOperation()
                              == SplitTableStrategyOperation.query) {
                            String tableColumns =
                                String.join(", ", splitTableStrategy.getColumns());
                            LOGGER.info("tableColumns = {}", tableColumns);
                            String sql =
                                String.format(
                                    Locale.ROOT,
                                    "SELECT %s FROM [%s].[%s].[%s] %s group by %s ",
                                    tableColumns,
                                    catalog,
                                    schema,
                                    tableName,
                                    SQL_DECORATOR_NON_DATA,
                                    tableColumns);
                            LOGGER.info("parentQuery = {}", sql);
                            SqlSelect sqlSelect = db.get().toSelect(sql);
                            List<String> sqlQueries = new ArrayList<>();
                            sqlSelect.query(
                                rs -> {
                                  while (rs.next()) {
                                    int index = 1;
                                    ResultSetMetaData resultMetadata = rs.getMetadata();
                                    String selectQueryPartial =
                                        String.format(
                                            Locale.ROOT,
                                            "SELECT %s FROM [%s].[%s].[%s] %s where ",
                                            columnSql,
                                            catalog,
                                            schema,
                                            tableName,
                                            SQL_DECORATOR);
                                    int columnCount = resultMetadata.getColumnCount();
                                    String whereClause = "";

                                    if (splitTableStrategy.getSubOperation()
                                        == SplitTableStrategyOperation.like) {
                                      String columnCondition =
                                          " "
                                              + splitTableStrategy.getSubColumn()
                                              + " "
                                              + splitTableStrategy.getSubOperation()
                                              + " '"
                                              + rs.getStringOrNull()
                                              + "%' ";
                                      whereClause = columnCondition;
                                    } else {
                                      for (int i = 1; i <= columnCount; i++) {
                                        String columnCondition = null;
                                        String columnName = resultMetadata.getColumnName(i);
                                        int type = resultMetadata.getColumnType(i);
                                        if (type == Types.VARCHAR || type == Types.CHAR) {
                                          columnCondition =
                                              " "
                                                  + columnName
                                                  + " = '"
                                                  + rs.getStringOrNull()
                                                  + "' ";
                                        } else if (type == Types.INTEGER) {
                                          columnCondition =
                                              " " + columnName + " = " + rs.getIntegerOrNull();
                                        }
                                        whereClause +=
                                            (index == 1)
                                                ? columnCondition
                                                : " AND " + columnCondition;
                                        index++;
                                      }
                                    }
                                    selectQueryPartial += whereClause;
                                    LOGGER.info("childQuery = {}", selectQueryPartial);
                                    sqlQueries.add(selectQueryPartial);
                                  }
                                  return sqlQueries;
                                });
                            listQueries.addAll(sqlQueries);
                          } else if (splitTableStrategy.getKeyDataType() == KeyDataType.date) {
                            listQueries.addAll(
                                createQueriesDate(
                                    splitTableStrategy, columnSql, catalog, schema, tableName));
                          } else if (splitTableStrategy.getKeyDataType() == KeyDataType.number) {
                            if (splitTableStrategy.getIncrement() == null
                                && splitTableStrategy.getOperation()
                                    == SplitTableStrategyOperation.between)
                              splitTableStrategy.setIncrement(
                                  (splitTableStrategy.getEndRange()
                                          - splitTableStrategy.getStartRange())
                                      / incrementFactor);
                            listQueries.addAll(
                                createQueriesNumber(
                                    splitTableStrategy, columnSql, catalog, schema, tableName));
                          } else if (splitTableStrategy.getKeyDataType() == KeyDataType.string) {
                            listQueries.addAll(
                                createQueriesString(
                                    splitTableStrategy, columnSql, catalog, schema, tableName));
                          }
                        });
                    if (splitTableStrategies.isEmpty()) {
                      String sql =
                          String.format(
                              Locale.ROOT,
                              "SELECT %s FROM [%s].[%s].[%s] %s",
                              columnSql,
                              catalog,
                              schema,
                              tableName,
                              SQL_DECORATOR_NO_WHERE);
                      listQueries.add(sql);
                      LOGGER.debug("(else) sql is {}", sql);
                    }
                    LOGGER.info(
                        "{}",
                        new Statistics(
                            "Created",
                            tableName,
                            listQueries.size(),
                            LocalDateTime.now(),
                            table.getDbRowCount()));

                    AtomicInteger index = new AtomicInteger(0);
                    table.setQueryCount(listQueries.size());
//                    LOGGER.warn(">>>>>>>>>>>>> GOT ALL QUERIES <<<<<<<<<<<<<");
                    return listQueries.stream()
                        .map(
                            query ->
                                new Query(
                                    table,
                                    query,
                                    StringUtils.leftPad(
                                        String.valueOf(index.incrementAndGet()), 7, "0"),
                                    "",
                                    ""))
                        .collect(Collectors.toList());
                  });
            })
        .flatMapIterable(l -> l);
  }

  private Long getDbRowCount(Database database, String catalog, String schema, String tableName) {
    String sql =
        String.format(
            Locale.ROOT,
            "SELECT sum(p.rows) AS TotalRowCount "
                + "FROM %1$s.sys.tables t "
                + "INNER JOIN %1$s.sys.partitions p ON t.object_id = p.OBJECT_ID and p.index_id IN (0, 1) "
                + "left join %1$s.sys.schemas s on s.schema_id = t.schema_id "
                + "WHERE s.name = '%2$s' "
                + "AND t.NAME = '%3$s'",
            catalog,
            schema,
            tableName);
    SqlSelect sqlSelect = database.toSelect(sql);
    return sqlSelect.query(
        rs -> {
          Long rc = null;
          while (rs.next()) {
            return rs.getLongOrNull();
          }
          return rc;
        });
  }

  /*
    ONLY SUPPORTED: splitTableStrategy.getOperation() == SplitTableStrategyOperation.range
  */
  private List<String> createQueriesDate(
      SplitTableStrategy splitTableStrategy,
      String columns,
      String catalog,
      String schema,
      String tableName) {
    List<String> lq = new ArrayList<>();
    long increment = splitTableStrategy.getIncrement();
    boolean continueLoop = true;

    int startRangeYear =
        LocalDate.parse(
                new SimpleDateFormat(DATE_FORMAT).format(splitTableStrategy.getStartRangeDate()))
            .getYear();
    int endRangeYear =
        LocalDate.parse(
                new SimpleDateFormat(DATE_FORMAT).format(splitTableStrategy.getEndRangeDate()))
            .getYear();

    long currentStartYear = startRangeYear;
    if (increment == 12) {
      long currentEndYear = startRangeYear + (increment / 12);
      String currentOperator = "<";
      String partialDateString = "-01-01 00::00::00'";
      do {
        String sqlQuery = null;

        // where report_dt report_dt < '2015-01-01 00:00:00' (first year)
        // where report_dt >= '2015-01-01 00:00:00' and report_dt < '2016-01-01 00:00:00' (middle
        // year)
        // where report_dt >= '2021-01-01 00:00:00'
        if (currentStartYear == startRangeYear) {
          sqlQuery =
              String.format(
                  Locale.ROOT,
                  "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s%s",
                  columns,
                  catalog,
                  schema,
                  tableName,
                  SQL_DECORATOR,
                  splitTableStrategy.getColumn(),
                  currentOperator,
                  currentEndYear,
                  partialDateString);
        } else if (currentStartYear > endRangeYear) {
          continueLoop = false;
        } else if (currentStartYear == endRangeYear) {
          currentOperator = ">=";
          sqlQuery =
              String.format(
                  Locale.ROOT,
                  "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s%s",
                  columns,
                  catalog,
                  schema,
                  tableName,
                  SQL_DECORATOR,
                  splitTableStrategy.getColumn(),
                  currentOperator,
                  currentStartYear,
                  partialDateString);
          continueLoop = false;
        } else {
          String currentOperator1 = ">=";
          String currentOperator2 = "<";
          sqlQuery =
              String.format(
                  Locale.ROOT,
                  "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s%s and %s %s '%s%s",
                  columns,
                  catalog,
                  schema,
                  tableName,
                  SQL_DECORATOR,
                  splitTableStrategy.getColumn(),
                  currentOperator1,
                  currentStartYear,
                  partialDateString,
                  splitTableStrategy.getColumn(),
                  currentOperator2,
                  currentEndYear,
                  partialDateString);
        }
        currentStartYear = currentEndYear;
        currentEndYear += (increment / 12);
        lq.add(sqlQuery);
        LOGGER.debug("(date) sql is {}", sqlQuery);

      } while (continueLoop);
    }
    // monthly
    else if (increment < 12) {
      int startRangeMonth =
          LocalDate.parse(
                  new SimpleDateFormat(DATE_FORMAT).format(splitTableStrategy.getStartRangeDate()))
              .getMonthValue();
      int endRangeMonth =
          LocalDate.parse(
                  new SimpleDateFormat(DATE_FORMAT).format(splitTableStrategy.getEndRangeDate()))
              .getMonthValue();

      long currentEndYear = startRangeYear;

      long currentStartMonth = startRangeMonth;
      long currentEndMonth = startRangeMonth + increment;
      if (currentEndMonth > 12) {
        currentEndMonth = currentEndMonth - 12;
        currentEndYear += 1;
      }

      String partialDateString = "-01 00::00::00'";
      do {
        String sqlQuery = null;

        // where report_dt report_dt < '2015-01-01 00:00:00' (first year)
        // where report_dt >= '2015-01-01 00:00:00' and report_dt < '2016-01-01 00:00:00' (middle
        // year)
        // where report_dt >= '2021-01-01 00:00:00'
        if (currentStartYear == startRangeYear && currentStartMonth == startRangeMonth) {
          String currentOperator = "<";
          sqlQuery =
              String.format(
                  Locale.ROOT,
                  "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s-%s%s",
                  columns,
                  catalog,
                  schema,
                  tableName,
                  SQL_DECORATOR,
                  splitTableStrategy.getColumn(),
                  currentOperator,
                  currentStartYear,
                  StringUtils.leftPad(String.valueOf(currentEndMonth), 2, "0"),
                  partialDateString);
        } else if ((currentStartYear > endRangeYear)
            || (currentStartYear == endRangeYear && currentStartMonth > endRangeMonth)) {
          continueLoop = false;
        } else if (currentStartYear == endRangeYear && currentStartMonth == endRangeMonth) {
          String currentOperator = ">=";
          sqlQuery =
              String.format(
                  Locale.ROOT,
                  "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s-%s%s",
                  columns,
                  catalog,
                  schema,
                  tableName,
                  SQL_DECORATOR,
                  splitTableStrategy.getColumn(),
                  currentOperator,
                  currentStartYear,
                  StringUtils.leftPad(String.valueOf(currentStartMonth), 2, "0"),
                  partialDateString);
          continueLoop = false;
        } else {
          String currentOperator1 = ">=";
          String currentOperator2 = "<";
          sqlQuery =
              String.format(
                  Locale.ROOT,
                  "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s-%s%s and %s %s '%s-%s%s",
                  columns,
                  catalog,
                  schema,
                  tableName,
                  SQL_DECORATOR,
                  splitTableStrategy.getColumn(),
                  currentOperator1,
                  currentStartYear,
                  StringUtils.leftPad(String.valueOf(currentStartMonth), 2, "0"),
                  partialDateString,
                  splitTableStrategy.getColumn(),
                  currentOperator2,
                  currentEndYear,
                  StringUtils.leftPad(String.valueOf(currentEndMonth), 2, "0"),
                  partialDateString);
        }
        currentStartMonth = currentEndMonth;
        currentEndMonth += increment;
        if (currentEndMonth > 12) {
          currentEndMonth = currentEndMonth - 12;
          currentEndYear += 1;
        }
        if (currentStartMonth > 12) {
          currentStartMonth = currentStartMonth - 12;
          currentStartYear += 1;
        }
        if (sqlQuery != null) {
          lq.add(sqlQuery);
        }
        LOGGER.debug("(date) sql is {}", sqlQuery);

      } while (continueLoop);
    }

    return lq;
  }

  /*
    ONLY SUPPORTED: splitTableStrategy.getKeyDataType() == KeyDataType.number
  */
  private List<String> createQueriesNumber(
      SplitTableStrategy splitTableStrategy,
      String columns,
      String catalog,
      String schema,
      String tableName) {
    List<String> lq = new ArrayList<>();
    long increment = splitTableStrategy.getIncrement();
    boolean continueLoop = true;
    long startRange = splitTableStrategy.getStartRange();
    long endRange = splitTableStrategy.getEndRange();
    long currentStart = startRange;
    long currentEnd = startRange + increment;
    String sqlQuery = null;

    do {
      if (splitTableStrategy.getOperation() == SplitTableStrategyOperation.like) {
        sqlQuery =
            String.format(
                Locale.ROOT,
                "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s%s%%'",
                columns,
                catalog,
                schema,
                tableName,
                SQL_DECORATOR,
                splitTableStrategy.getColumn(),
                splitTableStrategy.getOperation(),
                splitTableStrategy.getPrefix() == null ? "" : splitTableStrategy.getPrefix(),
                currentStart);
        currentStart += increment;
        if (currentStart > endRange) continueLoop = false;
      } else if (splitTableStrategy.getOperation() == SplitTableStrategyOperation.between) {
        sqlQuery =
            String.format(
                Locale.ROOT,
                "SELECT %s FROM [%s].[%s].[%s] %s where %s %s %s AND %s",
                columns,
                catalog,
                schema,
                tableName,
                SQL_DECORATOR,
                splitTableStrategy.getColumn(),
                splitTableStrategy.getOperation(),
                currentStart,
                currentEnd);
        currentStart = currentEnd + 1;
        currentEnd = Math.min(currentStart + increment, endRange);
        if (currentStart > endRange) continueLoop = false;
      }
      lq.add(sqlQuery);
      LOGGER.debug("(number) sql is {}", sqlQuery);
    } while (continueLoop);

    if (splitTableStrategy.getOperation() == SplitTableStrategyOperation.like) {
      // if a prefix is supplied, this a catch all query. This may result the cases where user is
      // adding the same table with multiple prefixes
      // another issue can be that a big query is generated
      if (splitTableStrategy.getPrefix() != null) {
        sqlQuery =
            String.format(
                Locale.ROOT,
                "SELECT %s FROM [%s].[%s].[%s] %s where %s not %s '%s%%'",
                columns,
                catalog,
                schema,
                tableName,
                SQL_DECORATOR,
                splitTableStrategy.getColumn(),
                splitTableStrategy.getOperation(),
                splitTableStrategy.getPrefix());
        lq.add(sqlQuery);
      }
      // a catch all query if the supplied start range number is > 1 digit otherwise first 10
      // records may be missed
      if (splitTableStrategy.getStartRange() >= 10) {
        sqlQuery =
            String.format(
                Locale.ROOT,
                "SELECT %s FROM [%s].[%s].[%s] %s where %s in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')",
                columns,
                catalog,
                schema,
                tableName,
                SQL_DECORATOR,
                splitTableStrategy.getColumn());
        lq.add(sqlQuery);
      }
      // TODO::a catch all query is needed for the case lowercase/uppercase
      // for cases dbo.CR_LOAD_STATUS_LOG_FILES,extract_file_name,string,like,1,A,Z
    }

    return lq;
  }

  /*
    ONLY SUPPORTED: splitTableStrategy.getKeyDataType() == KeyDataType.string
    ONLY SUPPORTED: splitTableStrategy.getOperation() == SplitTableStrategyOperation.like
    NOT SUPPORTED: splitTableStrategy.getOperation() == SplitTableStrategyOperation.between
  */
  private List<String> createQueriesString(
      SplitTableStrategy splitTableStrategy,
      String columns,
      String catalog,
      String schema,
      String tableName) {
    List<String> lq = new ArrayList<>();
    boolean continueLoop = true;
    char[] charArray = null;
    String currentStartString = null;
    int index = 0;
    String startRangeString = splitTableStrategy.getStartRangeString();
    String endRangeString = splitTableStrategy.getEndRangeString();
    charArray =
        IntStream.rangeClosed(startRangeString.charAt(0), endRangeString.charAt(0))
            .mapToObj(c -> "" + (char) c)
            .collect(Collectors.joining())
            .toCharArray();
    currentStartString = String.valueOf(charArray[index]);
    String sqlQuery = null;
    do {
      sqlQuery =
          String.format(
              Locale.ROOT,
              "SELECT %s FROM [%s].[%s].[%s] %s where %s %s '%s%%'",
              columns,
              catalog,
              schema,
              tableName,
              SQL_DECORATOR,
              splitTableStrategy.getColumn(),
              splitTableStrategy.getOperation(),
              currentStartString);
      index += splitTableStrategy.getIncrement();
      if (charArray.length > index) {
        currentStartString = String.valueOf(charArray[index]);
      } else continueLoop = false;
      lq.add(sqlQuery);
      LOGGER.debug("(string) sql is {}", sqlQuery);
    } while (continueLoop);
    return lq;
  }

  private String getColumnSql(Job job, Table table) {
    return table.getColumns().stream()
        .filter(Column::isExportable)
        .map(
            col -> {
              // Use column name string not JDBC type val to avoid sqlserver->jdbc mappings
              if (job.stringDatetime
                  && (col.vendorType.equals("datetime")
                      || col.vendorType.equals("datetime2")
                      || col.vendorType.equals("smalldatetime"))) {
                return String.format(
                    Locale.ROOT,
                    "CONVERT(varchar, [%s], %d) AS [%s%s]",
                    col.name,
                    STRING_DATE_CONVERSION,
                    col.name,
                    job.stringDatetimeSuffix);
              } else {
                return "[" + col.name + "]";
              }
            })
        .collect(Collectors.joining(", "));
  }
}
