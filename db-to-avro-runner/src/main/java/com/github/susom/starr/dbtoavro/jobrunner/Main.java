
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
import java.io.PrintStream;
import java.util.Arrays;
import java.util.UUID;
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
   * Main entrypoint
   *
   * @param args command line arguments
   */
  private void launch(String[] args) {

    try {

      //TODO: Cleanup, this is a quick-n-dirty command line parser
      OptionParser parser = new OptionParser();
      OptionSpec<String> preSql = parser.accepts("pre-sql", "sql to execute before restore").withRequiredArg();
      OptionSpec<String> postSql = parser.accepts("post-sql", "sql to execute after restore").withRequiredArg();
      OptionSpec<String> type = parser.accepts("type", "job type").withRequiredArg().required();
      OptionSpec<String> databaseType = parser.accepts("database-type", "database vendor").withRequiredArg().required();
      OptionSpec<String> catalog = parser.accepts("catalog", "catalogs to export").withRequiredArg().required();
      OptionSpec<String> schemas = parser.accepts("schemas", "schemas to export")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(',');
      OptionSpec<String> source = parser.accepts("source", "directory containing backups").withRequiredArg().required();
      OptionSpec<String> destination = parser.accepts("destination", "destination of output files").withRequiredArg()
          .required();
      OptionSpec<String> backupFiles = parser.accepts("backup-files", "comma-delimited list of backup files")
          .withRequiredArg()
          .ofType(String.class)
          .withValuesSeparatedBy(',')
          .required();
      OptionSpec<Void> helpOption = parser.acceptsAll(Arrays.asList("h", "help"), "show help").forHelp();

      OptionSet optionSet = null;
      try {
        optionSet = parser.parse(args);
      } catch (OptionException ex) {
        parser.printHelpOn(System.out);
        exit(1);
      }
      if (optionSet.has(helpOption)) {
        parser.printHelpOn(System.out);
        exit(0);
      }

      final Job job = new Builder()
          .id(0L)
          .type(optionSet.valueOf(type))
          .databaseType(optionSet.valueOf(databaseType))
          .catalog(optionSet.valueOf(catalog))
          .schemas(optionSet.valuesOf(schemas))
          .backupUri(optionSet.valueOf(source))
          .backupFiles(optionSet.valuesOf(backupFiles))
          .destination(optionSet.valueOf(destination))
          .preSql(optionSet.valueOf(preSql))
          .postSql(optionSet.valueOf(postSql))
          .build();

      Config config = readConfig();
      LOGGER.info("Configuration is being loaded from the following sources in priority order:\n" + config.sources());

      new ConsoleJobRunner(config, job)
          .run()
          .doOnError(error -> {
            System.err.println("Job failed!");
            error.printStackTrace();
            exit(1);
          })
          .blockingAwait();

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
