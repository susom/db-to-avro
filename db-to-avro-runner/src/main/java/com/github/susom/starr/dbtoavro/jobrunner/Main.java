
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
import com.github.susom.database.Flavor;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job.Builder;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
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
    OptionSpec<Flavor> flavor = parser.accepts("flavor", "database type (sqlserver, oracle)")
        .withRequiredArg()
        .required()
        .ofType(Flavor.class);
    OptionSpec<String> connection = parser.accepts("connect", "jdbc connection string for existing database")
        .withRequiredArg();
    OptionSpec<String> user = parser.accepts("user", "database user (existing db)")
        .requiredIf(connection)
        .withRequiredArg();
    OptionSpec<File> passwordFile = parser.accepts("password-file", "database password file (existing db)")
        .withRequiredArg()
        .ofType(File.class);
    OptionSpec<String> password = parser.accepts("password", "database password (existing db)")
        .withRequiredArg();
    OptionSpec<String> backupDir = parser.accepts("backup-dir", "directory containing backup to restore, mounted as /backup in container")
        .requiredUnless(connection)
        .withRequiredArg();
    OptionSpec<String> backupFiles = parser
        .accepts("backup-files", "comma-delimited list of .bak files (MSSQL), or a single .par file (Oracle)")
        .requiredIf(backupDir)
        .availableUnless(connection)
        .withRequiredArg()
        .ofType(String.class)
        .withValuesSeparatedBy(',');
    OptionSpec<String> destination = parser.accepts("destination", "avro destination directory").withRequiredArg();
    OptionSpec<String> catalog = parser.accepts("catalog", "catalog to export").withRequiredArg();
    OptionSpec<String> schemas = parser.accepts("schemas", "only export this comma-delimited list of schemas")
        .withRequiredArg()
        .ofType(String.class)
        .withValuesSeparatedBy(',');
    OptionSpec<String> tables = parser.accepts("tables", "only export this comma-delimited list of tables")
        .withRequiredArg()
        .ofType(String.class)
        .withValuesSeparatedBy(',');
    OptionSpec<String> filters = parser.accepts("exclude", "exclusions in form schema(.table)(.column)")
        .withRequiredArg()
        .ofType(String.class)
        .withValuesSeparatedBy(',');
    OptionSpec<String> preSql = parser.accepts("pre-sql", "path of sql file to execute before restore")
        .availableUnless(connection)
        .withRequiredArg();
    OptionSpec<String> postSql = parser.accepts("post-sql", "path of sql file to execute after restore")
        .availableUnless(connection)
        .withRequiredArg();
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
          .tables(optionSet.valuesOf(tables))
          .exclusions(optionSet.valuesOf(filters))
          .backupDir(optionSet.valueOf(backupDir))
          .backupFiles(optionSet.has(backupFiles)
              ? optionSet.valuesOf(backupFiles)
              : (optionSet.has(backupDir)
                  ? Files.list(Paths.get(optionSet.valueOf(backupDir))).filter(Files::isRegularFile).map(Path::toString)
                  .collect(Collectors.toList())
                  : null))
          .destination(optionSet.valueOf(destination))
          .preSql(optionSet.valueOf(preSql))
          .postSql(optionSet.valueOf(postSql))
          .connection(optionSet.valueOf(connection))
          .timezone(System.getProperty("user.timezone"))
          .build();

      Config config = readConfig();
      if (job.connection == null) {
        config = Config.from().config(config)
            .value("database.url", config.getString(job.flavor + ".database.url"))
            .value("database.user", config.getString(job.flavor + ".database.user"))
            .value("database.password", config.getString(job.flavor + ".database.password")).get();
      } else {
        ConfigFrom conf = Config.from()
            .config(config)
            .value("database.url", optionSet.valueOf(connection))
            .value("database.user", optionSet.valueOf(user));
        if (optionSet.valueOf(password) != null) {
          conf = conf.value("database.password", optionSet.valueOf(password));
        }
        if (optionSet.valueOf(passwordFile) != null) {
          String pass = new String(Files.readAllBytes(optionSet.valueOf(passwordFile).toPath()), Charset.defaultCharset());
          conf = conf.value("database.password", pass);
        }
        config = conf.get();
      }

      LOGGER.info("Configuration is being loaded from the following sources in priority order:\n" + config.sources());

      // Really stress the importance of the timezone setting for the JVM
      String tz = System.getProperty("user.timezone");
      LOGGER.info("System time zone is {}, which will be used for dates with no timezone information.", tz);
      LOGGER.info("Set the -Duser.timezone= property if the source database is not {}.", tz);

      long start;
      start = System.nanoTime();
      new JobRunner(config, job)
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
        .substitutions(subs).get();
  }

}
