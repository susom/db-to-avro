
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

package com.github.susom.starr.dbtoavro.jobrunner;

import static java.lang.System.exit;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job.Builder;
import com.github.susom.starr.dbtoavro.jobrunner.runner.ConsoleJobRunner;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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

  public static void main(String[] args) {
    // Make sure we use the real console for error logging here because something
    // might have gone wrong during log config or console redirection
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
    OptionSpec<String> preSql = parser.accepts("pre-sql", "sql to execute before restore").withRequiredArg();
    OptionSpec<String> postSql = parser.accepts("post-sql", "sql to execute after restore").withRequiredArg();
    OptionSpec<String> catalog = parser.accepts("catalog", "catalogs to export").withRequiredArg().required();
    OptionSpec<String> schemas = parser.accepts("schemas", "schemas to export")
        .withRequiredArg()
        .ofType(String.class)
        .withValuesSeparatedBy(',');
    OptionSpec<String> connection = parser.accepts("connect", "jdbc connection string for existing database")
        .withRequiredArg();
    OptionSpec<String> user = parser.accepts("user", "database user")
        .requiredIf(connection)
        .withRequiredArg();
    OptionSpec<String> password = parser.accepts("password", "database password")
        .requiredIf(connection)
        .withRequiredArg();
    OptionSpec<String> backupDir = parser.accepts("backup-dir", "directory containing backup to restore")
        .requiredUnless(connection)
        .withRequiredArg();
    OptionSpec<String> backupFiles = parser.accepts("backup-files", "comma-delimited list of backup files")
        .requiredIf(backupDir)
        .availableUnless(connection)
        .withRequiredArg()
        .ofType(String.class)
        .withValuesSeparatedBy(',');
    OptionSpec<String> flavor = parser.accepts("flavor", "database type (sqlserver, oracle)")
        .withRequiredArg()
        .required();
    OptionSpec<String> destination = parser.accepts("destination", "avro destination directory").withRequiredArg()
        .required();
    OptionSpec<Void> helpOption = parser.acceptsAll(Arrays.asList("h", "help"), "show help").forHelp();

    try {

      OptionSet optionSet = parser.parse(args);

      if (optionSet.has(helpOption)) {
        parser.printHelpOn(System.out);
        exit(0);
      }

      final Job job = new Builder()
          .id(0L)
          .flavor(optionSet.valueOf(flavor))
          .catalog(optionSet.valueOf(catalog))
          .schemas(optionSet.valuesOf(schemas))
          .backupDir(optionSet.valueOf(backupDir))
          .backupFiles(optionSet.has(backupFiles)
              ? optionSet.valuesOf(backupFiles)
              : (optionSet.has(backupDir) ?
                  Files.list(Paths.get(optionSet.valueOf(backupDir))).filter(Files::isRegularFile).map(Path::toString)
                      .collect(Collectors.toList())
                  : null))
          .destination(optionSet.valueOf(destination))
          .preSql(optionSet.valueOf(preSql))
          .postSql(optionSet.valueOf(postSql))
          .connection(optionSet.valueOf(connection))
          .build();

      Config config = readConfig();
      if (job.connection == null) {
        config = Config.from().config(config)
            .value("database.url", config.getString(job.flavor + ".database.url"))
            .value("database.user", config.getString(job.flavor + ".database.user"))
            .value("database.password", config.getString(job.flavor + ".database.password")).get();
      } else {
        config = Config.from()
            .config(config)
            .value("database.url", optionSet.valueOf(connection))
            .value("database.user", optionSet.valueOf(user))
            .value("database.password", optionSet.valueOf(password))
            .get();
      }

      LOGGER.info("Configuration is being loaded from the following sources in priority order:\n" + config.sources());

      new ConsoleJobRunner(config, job)
          .run()
          .doOnError(error -> {
            System.err.println("Job failed!");
            error.printStackTrace();
            exit(1);
          })
          .blockingAwait();

    } catch (OptionException ex) {
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

    Config subs = ConfigFrom.firstOf().value("UUID", UUID.randomUUID().toString()).get();

    LOGGER.debug("UUID: {}", subs.getString("UUID"));
    return Config.from().systemProperties().propertyFile(properties.split(File.pathSeparator))
        .substitutions(subs).get();
  }

}
