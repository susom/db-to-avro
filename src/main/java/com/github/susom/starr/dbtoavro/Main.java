
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

package com.github.susom.starr.dbtoavro;

import static java.lang.System.exit;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.database.Flavor;
import com.github.susom.starr.dbtoavro.entity.Job.Builder;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static final int DEFAULT_FETCH_COUNT = 2000;
  private static final String DEFAULT_DATETIME_COLUMN_SUFFIX = "__dt_str";
  private static final String DEFAULT_AVRO_CODEC = "snappy";
  private static final String DEFAULT_FILENAME_PATTERN = "%{SCHEMA}.%{TABLE}.avro";

  public static void main(String[] args) {
    // Make sure we use the real console for error logging here because something
    // might have gone wrong during notify config or console redirection
    PrintStream err = System.err;
    try {
      new Main().launch(args);
    } catch (Throwable t) {
      t.printStackTrace(err);
      err.println("Exiting with error code 1");
      System.exit(1);
    }
  }

  /**
   * Main entry point
   *
   * @param args command line arguments
   */
  private void launch(String[] args) throws IOException {

    OptionParser parser = new OptionParser();
    OptionSpec<String> flavorOpt = parser.accepts("flavor", "database type (sqlserver, oracle)")
      .withRequiredArg()
      .required()
      .ofType(String.class);

    OptionSpec<String> connectionOpt = parser.accepts("connect", "jdbc connection string for existing database")
      .withRequiredArg();

    OptionSpec<String> userOpt = parser.accepts("user", "database user (existing db)")
      .requiredIf(connectionOpt)
      .withRequiredArg();

    OptionSpec<File> passwordFileOpt = parser.accepts("password-file", "database password file (existing db)")
      .withRequiredArg()
      .ofType(File.class);

    OptionSpec<String> passwordOpt = parser.accepts("password", "database password (existing db)")
      .withRequiredArg();

    OptionSpec<String> backupDirOpt = parser
      .accepts("backup-dir", "directory containing backup files to restore, mounted as /backup in container")
      .requiredUnless(connectionOpt)
      .withRequiredArg();

    OptionSpec<String> backupFilesOpt = parser
      .accepts("backup-files", "comma-delimited list of .bak files (MSSQL), or a single .par file (Oracle)")
      .requiredIf(backupDirOpt)
      .availableUnless(connectionOpt)
      .withRequiredArg()
      .ofType(String.class)
      .withValuesSeparatedBy(',');

    OptionSpec<String> destinationOpt = parser.accepts("destination", "avro destination directory").withRequiredArg();

    OptionSpec<String> catalogOpt = parser.accepts("catalog", "catalog to export").withRequiredArg();

    OptionSpec<String> schemasOpt = parser.accepts("schema", "only export this comma-delimited list of schemas")
      .withRequiredArg()
      .ofType(String.class)
      .withValuesSeparatedBy(',');

    OptionSpec<String> tablesOpt = parser.accepts("table", "only export this comma-delimited list of schema.table")
      .withRequiredArg()
      .ofType(String.class)
      .withValuesSeparatedBy(',');

    OptionSpec<String> tablePrioritiesOpt = parser.accepts("prioritize-table", "comma-delimited list of schema.table that should be exported first")
      .withRequiredArg()
      .ofType(String.class)
      .withValuesSeparatedBy(',');

    OptionSpec<String> tableExclusionsOpt = parser.accepts("exclude-table", "case-insensitive regex filter matched against schema.table")
      .withRequiredArg()
      .ofType(String.class)
      .withValuesSeparatedBy(',');

    OptionSpec<String> columnExclusionsOpt = parser.accepts("exclude-column", "case-insensitive regex filter matched against schema.table.column")
      .withRequiredArg()
      .ofType(String.class)
      .withValuesSeparatedBy(',');

    OptionSpec<String> preSqlOpt = parser.accepts("pre-sql", "path of sql file to execute before restore")
      .availableUnless(connectionOpt)
      .withRequiredArg();

    OptionSpec<String> postSqlOpt = parser.accepts("post-sql", "path of sql file to execute after restore")
      .availableUnless(connectionOpt)
      .withRequiredArg();

    OptionSpec<Boolean> stringDatetimeOpt = parser
      .accepts("datetime-string", "Convert Oracle(Date) or SQLServer(DateTime) types to ISO-8601 string")
      .withRequiredArg()
      .ofType(Boolean.class);

    OptionSpec<String> datetimeStringSuffixOpt = parser.accepts("datetime-string-suffix",
      "Append this column name suffix to columns that have been converted with --datetime-string")
      .withRequiredArg()
      .ofType(String.class);

    OptionSpec<String> filenamePatternOpt = parser.accepts("filename-pattern",
      "File naming pattern, default is %{SCHEMA}.%{TABLE}-%{PART}.avro")
      .withRequiredArg()
      .ofType(String.class);

    OptionSpec<Integer> fetchRowCountOpt = parser.accepts("fetch-row-count",
      String.format(Locale.ROOT, "Number of rows to fetch from DB per query (default %d)", DEFAULT_FETCH_COUNT))
      .withRequiredArg()
      .ofType(Integer.class);

    OptionSpec<String> avroCodecOpt = parser
      .accepts("avro-codec",
        String.format(Locale.ROOT, "Avro compression: null, snappy, deflate (default %s)", DEFAULT_AVRO_CODEC))
      .withRequiredArg()
      .ofType(String.class);

    OptionSpec<String> logFileOpt = parser
      .accepts("log-file",  "Output JSON log file path")
      .withRequiredArg()
      .ofType(String.class);

    OptionSpec<Integer> avroSizeOpt = parser
      .accepts("avro-size", "Target .avro file size before splitting (default: unlimited)")
      .withRequiredArg()
      .ofType(Integer.class);

    OptionSpec<Integer> threadsOpt = parser
      .accepts("threads", "Number of parallel threads (default is core count)")
      .withRequiredArg()
      .ofType(Integer.class);

    OptionSpec<Boolean> tidyOpt = parser.accepts("tidy-tables", "Normalize table names (columns are always normalized)")
      .withRequiredArg()
      .ofType(Boolean.class);

    OptionSpec<Void> helpOption = parser.acceptsAll(Arrays.asList("h", "help"), "show help").forHelp();

    try {

      OptionSet optionSet = parser.parse(args);

      if (optionSet.has(helpOption)) {
        parser.printHelpOn(System.out);
        exit(0);
      }

      // Configuration Properties can be overridden by command-line options
      Config config = readConfig();

      int fetchRowCount = config.getInteger("fetch.row.count", DEFAULT_FETCH_COUNT);
      if (optionSet.has(fetchRowCountOpt)) {
        fetchRowCount = optionSet.valueOf(fetchRowCountOpt);
      }

      boolean stringDatetime = config.getBooleanOrFalse("datetime.string");
      if (optionSet.has(stringDatetimeOpt)) {
        stringDatetime = optionSet.valueOf(stringDatetimeOpt);
      }

      String stringDatetimeSuffix = config.getString("datetime.string.suffix", DEFAULT_DATETIME_COLUMN_SUFFIX);
      if (optionSet.has(datetimeStringSuffixOpt)) {
        stringDatetimeSuffix = optionSet.valueOf(datetimeStringSuffixOpt);
      }

      String filenamePattern = config.getString("filename.pattern", DEFAULT_FILENAME_PATTERN);
      if (optionSet.has(filenamePatternOpt)) {
        filenamePattern = optionSet.valueOf(filenamePatternOpt);
      }

      String flavor = optionSet.valueOf(flavorOpt).toLowerCase(Locale.ROOT);

      String codec = config.getString("avro.codec", DEFAULT_AVRO_CODEC).toLowerCase(Locale.ROOT);
      if (optionSet.has(avroCodecOpt)) {
        codec = optionSet.valueOf(avroCodecOpt).toLowerCase(Locale.ROOT);
      }
      switch (codec) {
        case "null":
        case "snappy":
        case "deflate":
          break;
        default:
          parser.printHelpOn(System.out);
          System.err.println("\nInvalid Avro compression codec specified");
          exit(1);
      }

      boolean tidyTables = config.getBooleanOrFalse("tidy.table.names");
      if (optionSet.has(tidyOpt)) {
        tidyTables = optionSet.valueOf(tidyOpt);
      }

      int avroSize = config.getInteger("avro.size", 0);
      if (optionSet.has(avroSizeOpt)) {
        avroSize = optionSet.valueOf(avroSizeOpt);
      }

      String logFile = config.getString("logfile");
      if (optionSet.has(logFileOpt)) {
        logFile = optionSet.valueOf(logFileOpt);
      }

      // Keep some database connection strings as properties-only, so they don't show up in job logs
      ConfigFrom finalConfiguration = Config.from().config(config);

      String connection = config.getString(flavor + ".database.url");
      if (optionSet.has(connectionOpt)) {
        if (connection != null) {
          parser.printHelpOn(System.out);
          System.err.println("\nCommand-line connection string cannot override properties file");
          exit(1);
        } else {
          finalConfiguration.value("database.url", optionSet.valueOf(connectionOpt));
        }
      }

      if (optionSet.has(userOpt)) {
        String databaseUser = config.getString(flavor + ".database.user");
        if (databaseUser != null) {
          parser.printHelpOn(System.out);
          System.err.println("\nCommand-line database username cannot override properties file");
          exit(1);
        } else {
          finalConfiguration.value("database.user", optionSet.valueOf(userOpt));
        }
      }

      String databasePassword = config.getString(flavor + ".database.password");
      if (optionSet.has(passwordOpt)) {
        if (databasePassword != null) {
          parser.printHelpOn(System.out);
          System.err.println("\nCommand-line database password cannot override properties file");
          exit(1);
        } else {
          databasePassword = optionSet.valueOf(passwordOpt);
        }
      } else if (optionSet.has(passwordFileOpt)) {
        if (databasePassword != null) {
          parser.printHelpOn(System.out);
          System.err.println("\nCommand-line database password file cannot override properties file");
          exit(1);
        } else {
          databasePassword = new String(Files.readAllBytes(optionSet.valueOf(passwordFileOpt).toPath()),
            Charset.defaultCharset());
        }
      }
      finalConfiguration.value("database.password", databasePassword);

      int threads = config.getInteger("database.pool.size", Runtime.getRuntime().availableProcessors());
      if (optionSet.has(threadsOpt)) {
        threads = optionSet.valueOf(threadsOpt);
      }
      finalConfiguration.value("database.pool.size", String.valueOf(threads));

      config = finalConfiguration.get();

      // Create job based on computed configuration values
      final Builder jobBuilder = new Builder()
        .id(config.getString("UUID"))
        .flavor(Flavor.valueOf(flavor))
        .catalog(optionSet.valueOf(catalogOpt))
        .schemas(optionSet.valuesOf(schemasOpt))
        .tables(optionSet.valuesOf(tablesOpt))
        .tablePriorities(optionSet.valuesOf(tablePrioritiesOpt))
        .tableExclusions(optionSet.valuesOf(tableExclusionsOpt))
        .columnExclusions(optionSet.valuesOf(columnExclusionsOpt))
        .backupDir(optionSet.valueOf(backupDirOpt))
        .backupFiles(optionSet.has(backupFilesOpt)
          ? optionSet.valuesOf(backupFilesOpt)
          : (optionSet.has(backupDirOpt)
            ? Files.list(Paths.get(optionSet.valueOf(backupDirOpt))).filter(Files::isRegularFile).map(Path::toString)
            .collect(Collectors.toList())
            : null))
        .destination(optionSet.valueOf(destinationOpt))
        .preSql(optionSet.valueOf(preSqlOpt))
        .postSql(optionSet.valueOf(postSqlOpt))
        .timezone(System.getProperty("user.timezone"))
        .fetchRows(fetchRowCount)
        .avroSize(avroSize)
        .stringDatetime(stringDatetime)
        .stringDatetimeSuffix(stringDatetimeSuffix)
        .tidyTables(tidyTables)
        .codec(codec)
        .filenamePattern(filenamePattern)
        .logfile(logFile);

      LOGGER.info("Configuration is being loaded from the following sources in priority order:\n" + config.sources());

      // Really stress the importance of the timezone setting for the JVM
      String tz = System.getProperty("user.timezone");
      LOGGER.info("System time zone is {}, which will be used for dates with no timezone information.", tz);
      LOGGER.info("Set the -Duser.timezone= property if the source database is not {}.", tz);

      // Create destination directory if it doesn't already exist
      File destDir = new File(optionSet.valueOf(destinationOpt));
      if (destDir.isFile()) {
        parser.printHelpOn(System.out);
        System.err.println("\nDestination must be a directory");
        exit(1);
      }
      if (!(destDir.exists())) {
        destDir.mkdirs();
      }

      long start;
      start = System.nanoTime();
      new JobRunner(config, jobBuilder.build())
        .run()
        .doOnError(error -> {
          System.err.println("Job failed!");
          error.printStackTrace();
          exit(1);
        })
        .blockingAwait();
      System.out.println("Elapsed time: " + (System.nanoTime() - start) / 1000000000 + " seconds");

    } catch (OptionException ex) {
      System.out.println(ex.getMessage());
      parser.printHelpOn(System.out);
      exit(1);
    } catch (Exception ex) {
      ex.printStackTrace();
      exit(1);
    }

  }

  private Config readConfig() {
    String properties = System.getProperty("properties",
      "conf/app.properties" + File.pathSeparator + "local.properties" + File.pathSeparator + "sample.properties");

    String uuid = UUID.randomUUID().toString();
    Config subs = ConfigFrom.firstOf().value("UUID", uuid.substring(0, 18)).get();
    LOGGER.debug("UUID: {}", subs.getString("UUID"));

    return Config.from()
      .propertyFile(properties.split(File.pathSeparator))
      .systemProperties()
      .substitutions(subs)
      .excludeKeys("database.pool.size") // we calculate this
      .get();
  }

}
