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
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.entity.Query;
import com.github.susom.starr.dbtoavro.entity.Statistics;
import com.github.susom.starr.dbtoavro.entity.Table;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx.Builder;
import com.google.common.collect.Lists;

import io.reactivex.Observable;
import io.reactivex.Single;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.sql.Connection;
import java.sql.CallableStatement;
import oracle.jdbc.OracleTypes;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle specific SQL statements, for various database tasks
 */
public class OracleDatabaseFns extends DatabaseFns {

  private static final Logger LOGGER = LoggerFactory.getLogger(OracleDatabaseFns.class);
  private static final String STRING_DATE_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS";
  private int BATCH_SIZE = 100;
  private static final String partialSql =  "			SELECT ' WHERE ROWID BETWEEN ''' " +
                                            "			   || DBMS_ROWID.ROWID_CREATE(1, DATA_OBJECT_ID, LO_FNO, LO_BLOCK, 0) " +
                                            "			   || ''' AND ''' " +
                                            "			   || DBMS_ROWID.ROWID_CREATE(1, DATA_OBJECT_ID, HI_FNO, HI_BLOCK, 10000) " +
                                            "			   || '''' " +
                                            "				BULK COLLECT INTO query_collection_string " +
                                            "				FROM ( " +
                                            "			   SELECT DISTINCT GRP, " +
                                            "							 FIRST_VALUE(RELATIVE_FNO) OVER (PARTITION BY GRP ORDER BY RELATIVE_FNO, BLOCK_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)         LO_FNO, " +
                                            "							 FIRST_VALUE(BLOCK_ID) OVER (PARTITION BY GRP ORDER BY RELATIVE_FNO, BLOCK_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)             LO_BLOCK, " +
                                            "							 LAST_VALUE(RELATIVE_FNO) OVER (PARTITION BY GRP ORDER BY RELATIVE_FNO, BLOCK_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)          HI_FNO, " +
                                            "							 LAST_VALUE(BLOCK_ID + BLOCKS - 1) OVER (PARTITION BY GRP ORDER BY RELATIVE_FNO, BLOCK_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) HI_BLOCK, " +
                                            "							 SUM(BLOCKS) OVER (PARTITION BY GRP)                                                                                                                SUM_BLOCKS " +
                                            "			    FROM (SELECT RELATIVE_FNO, " +
                                            "						  BLOCK_ID, " +
                                            "						  BLOCKS, " +
                                            "							TRUNC((SUM(BLOCKS) OVER (ORDER BY RELATIVE_FNO, BLOCK_ID) - 0.01) / (SUM(BLOCKS) OVER () / 256)) GRP " +
                                            "					    FROM DBA_EXTENTS ";

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

  private List<Column> retrieveColumns(DatabaseMetaData metadata, String catalog, String schema, String table,
    List<String> columnExclusions) throws SQLException {
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
    return cols;
  }

  /**
   * Not used in Oracle
   */
  @Override
  public Single<String> getRestoreSql(String catalog, List<String> backupFiles) {
    return null;
  }

  @Override
  public Observable<String> getTables(String schema, Job job) {
    String catalog = job.catalog;
    List<String> tablesSplit = job.tablesSplit;
    List<String> priorities = job.tablePriorities;

    return dbb.transactRx(db -> {
      List<String> schemaTables = new ArrayList<String>();

      schemaTables.addAll(priorities.stream()
        .filter(t -> t.startsWith(schema + "."))
        .map(t -> t.split("\\.")[1])
        .collect(Collectors.toList()));

      if (null != tablesSplit)
      {
        schemaTables.addAll(tablesSplit.stream()
          .filter(t -> t.startsWith(schema + ".") && !schemaTables.contains(t.split("\\.")[1]))
          .map(t -> t.split("\\.")[1])
          .collect(Collectors.toList()));
      }

      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      try (ResultSet tables = metadata.getTables(catalog, schema, null, new String[]{"TABLE"})) {
        List<String> tablesList = new ArrayList<>(schemaTables);
        List<String> schemaTableList = new ArrayList<>();
        while (tables.next()) {
          String tableName = tables.getString(3);
          schemaTableList.add(tableName);
          if (!tablesList.contains(tableName)) {
            tablesList.add(tableName);
          }
        }

        List<String> finalTableList = tablesList.stream().filter(o1 -> schemaTableList.stream().anyMatch(o2 -> o2.equals(o1)))
          .collect(Collectors.toList());

        return finalTableList;
      }
    }).toObservable().flatMapIterable(l -> l);
  }

  @Override
  public Observable<Query> getQueries(String schema, String tableName, Job job) {
    String catalog = job.catalog;
    List<String> columnExclusions = job.columnExclusions;
    
    return dbb.transactRx((db, tx) -> {
      tx.setRollbackOnError(false);
      tx.setRollbackOnly(false);
      String conditionalQuery = null;
      String sqlReturn = null;
      List<String> listQueries = new ArrayList<>();
      List<Query> queries = new ArrayList<>();
      db.get().underlyingConnection().setSchema(schema);
      DatabaseMetaData metadata = db.get().underlyingConnection().getMetaData();
      List<Column> cols = retrieveColumns(metadata, catalog, schema, tableName, columnExclusions);

      if (cols.stream().noneMatch(Column::isExportable)) {
        LOGGER.warn("Skipping table {}, no columns are exportable", tableName);
        return queries;
      }

      Table table = new Table(catalog, schema, tableName, cols);
      String columnSql = getColumnSql(job, table);

      if (job.tablesSplit.contains(schema + "." + tableName)) { //} || job.tablePriorities.contains(schema + "." + tableName)) {
        conditionalQuery = 
        partialSql +
        "					   WHERE OWNER || '.' || SEGMENT_NAME = UPPER(t_owner||'.'||t_name) " +
        "					   ORDER BY BLOCK_ID) " +
        "			 ), " +
        "		 (SELECT DATA_OBJECT_ID FROM DBA_OBJECTS WHERE OWNER || '.' || OBJECT_NAME = UPPER(t_owner||'.'||t_name)) ;";
        sqlReturn =  "	query_collection_string_main := query_collection_string_main MULTISET UNION query_collection_string; "+
                    " open l_rc for select column_value from table(query_collection_string_main);"; 
      }
      else {
        conditionalQuery = " ";
        sqlReturn =  "	query_collection_string_main :=" + 
          "VARCHAR2_NTT('" + 
          " " +
          "')" +
          ";" +
          " open l_rc for select column_value from table(query_collection_string_main);"; 

      }
      try {
        String plsql =
          "DECLARE "+
          " l_part    	varchar2(3); " +
          " l_subpart    varchar2(1); " +
          " t_owner varchar2(20) := null; " +
          " t_name varchar2(100) := null; " + 
          " l_rc sys_refcursor;" + 

          " CURSOR partitioned(t_owner varchar2, t_name varchar2) IS " +
          " SELECT partition_name FROM dba_tab_partitions " +
          " WHERE table_owner = t_owner " +
          " AND   table_name = t_name " +
          " ORDER BY partition_position DESC; " +

          " CURSOR sub_partitioned(t_owner varchar2, t_name varchar2, part_name varchar2) is " +
          " SELECT subpartition_name FROM dba_tab_subpartitions s " +
          " WHERE s.table_owner = t_owner " +
          " AND   s.table_name = t_name " +
          " AND   s.partition_name = part_name; " +

          " query_collection_string VARCHAR2_NTT := VARCHAR2_NTT(); " +
          " query_collection_string_main VARCHAR2_NTT := VARCHAR2_NTT();  " +

          " BEGIN " +
          "   t_owner := ?; " +
          "   t_name := ?; " +
          "	  SELECT partitioned INTO l_part FROM dba_tables WHERE owner = t_owner AND table_name = t_name; " +
          "	  IF l_part = 'YES' THEN " +
          "	  BEGIN " +
          "		   SELECT 'Y' INTO l_subpart FROM dba_tab_subpartitions  " +
          "		   WHERE  table_owner=t_owner  " +
          "		   AND    table_name=t_name " +
          "		   AND   rownum=1; " +
          "	  EXCEPTION WHEN no_data_found THEN " +
          "			l_subpart:='N'; " +
          "	  END; " + 
          "	  IF l_subpart='Y' THEN /* if Sub Partitioned */ " +
          "		   FOR i IN partitioned(t_owner, t_name) " +
          "			 LOOP " +                      
          "				 FOR j in sub_partitioned(t_owner, t_name, i.partition_name) " +
          "				 LOOP "+
          partialSql +
          "					 WHERE OWNER || '.' || SEGMENT_NAME = UPPER(t_owner||'.'||t_name) and partition_name = j.subpartition_name " +
          "					 ORDER BY BLOCK_ID) " +
          "			     ), " +
          "			     (SELECT DATA_OBJECT_ID FROM DBA_OBJECTS WHERE OWNER || '.' || OBJECT_NAME = UPPER(t_owner||'.'||t_name) and subobject_name = j.subpartition_name ); " +
          "			   query_collection_string_main  := query_collection_string_main MULTISET UNION query_collection_string; " + 
          "			   END LOOP; /* End loop of Sub Partitioned cursor */ " +
          "		   END LOOP; /* End loop of Partitioned Cursor */		" +
          "     open l_rc for select column_value from table(query_collection_string_main);" +
          "	   ELSE /* it means only Partitioned no Sub Partitions */ " +
          "		   FOR i IN partitioned(t_owner, t_name) " +
          "		   LOOP " +
          partialSql +
				  "        WHERE OWNER || '.' || SEGMENT_NAME = UPPER(t_owner||'.'||t_name) and partition_name=i.partition_name " +
				  "        ORDER BY BLOCK_ID) " +
		      "        ), " +
		      "        (SELECT DATA_OBJECT_ID FROM DBA_OBJECTS WHERE OWNER || '.' || OBJECT_NAME = UPPER(t_owner||'.'||t_name) and subobject_name=i.partition_name);" +	  
          "			   query_collection_string_main  := query_collection_string_main MULTISET UNION query_collection_string; " + 
		      "      END LOOP; /* End loop of Partitioned Cursor */       " +
          "     open l_rc for select column_value from table(query_collection_string_main);"+
		      "    END IF; /* End if of l_Subpart='Y' */ " +
          "	   ELSE  " +
          conditionalQuery +
          sqlReturn +
          "	  END IF; /* END IF of L_PART='Y' */   " +
          " ? := l_rc; " +
          "	End; ";
        Connection c = db.get().underlyingConnection();
        CallableStatement cs = c.prepareCall(plsql);
        cs.setString(1, schema);
        cs.setString(2, tableName);
        cs.registerOutParameter(3, OracleTypes.CURSOR);
        cs.execute();           
        ResultSet cursorResultSet = (ResultSet) cs.getObject(3);
        while (cursorResultSet.next ())
        {  
          listQueries.add(cursorResultSet.getString(1));
        }
        cs.close();

        List<String> updatedQueries = listQueries.stream()
          .map(partialSql -> String.format(Locale.ROOT, "SELECT %s FROM \"%s\".\"%s\" %s", columnSql, schema, tableName, partialSql))
          .collect(Collectors.toList());

        //updatedQueries.forEach(System.out::println);
        AtomicInteger index = new AtomicInteger(0);
        if (job.unionizeQuery.contains(schema + "." + tableName)) {
          List<List<String>> batches = Lists.partition(updatedQueries, BATCH_SIZE);
          List<String> unionizeQueries = new ArrayList<String>();
        
          batches.stream().forEach(batch-> {
            String result = batch.stream().collect(Collectors.joining(" UNION \n"));        
            unionizeQueries.add(result);          
          });
          table.setQueryCount(unionizeQueries.size());
          queries = unionizeQueries.stream().collect(Collectors.mapping(query -> 
            new Query(table, query, StringUtils.leftPad(String.valueOf(index.incrementAndGet()), 7, "0")
            , getId(query, startPattern).replace('/', '_').replace('+', '_')
            , getId(query, endPattern).replace('/', '_').replace('+', '_')), Collectors.toList()));
        }
        else {
          table.setQueryCount(updatedQueries.size());
          queries = updatedQueries.stream().collect(Collectors.mapping(query -> 
            new Query(table, query, StringUtils.leftPad(String.valueOf(index.incrementAndGet()), 7, "0")
            , getId(query, startPattern).replace('/', '_').replace('+', '_')
            , getId(query, endPattern).replace('/', '_').replace('+', '_')), Collectors.toList()));
        }
        LOGGER.debug("Table {} Number of queries {}", tableName, index.get());
        LOGGER.info("{}", new Statistics("Created", tableName, queries.size(), LocalDateTime.now(), table.getDbRowCount()));

      }
      catch(Exception e) {
        LOGGER.error("getQueries - tableName {}. Exception: {}", tableName, e);
        if (!job.continueOnException)        
          throw new Exception(String.format(Locale.ROOT, "getQueries - tableName %s. Exception: %s)", tableName, e));
      }
      return queries;
     
    }).toObservable().flatMapIterable(l -> l);
  }

  private String getColumnSql(Job job, Table table) {
    return table.getColumns().stream()
      .filter(Column::isExportable)
      .map(c -> {
        // Use column name string (DATE) not java.sql.Type since JDBC is TIMESTAMP
        if (job.stringDatetime && c.vendorType.equals("DATE")) {
          return String.format(Locale.ROOT, "TO_CHAR(\"%s\", '%s') AS \"%s%s\"",
            c.name,
            STRING_DATE_FORMAT.replace(":", "::"),
            c.name,
            job.stringDatetimeSuffix);
        } else {
          return "\"" + c.name + "\"";
        }
      })
      .collect(Collectors.joining(", "));
  }

  private static Pattern startPattern = Pattern.compile("ROWID BETWEEN '(.*?)'");
  private static Pattern endPattern = Pattern.compile("' AND '(.*?)'");

  private static String getId(String input, Pattern pattern) {
    Matcher matcher = pattern.matcher(input);
    return matcher.find() ? matcher.group(1) : "";
  }

}
