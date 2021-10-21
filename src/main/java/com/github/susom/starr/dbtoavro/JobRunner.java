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

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobs.impl.OracleLoadDataPump;
import com.github.susom.starr.dbtoavro.jobs.impl.AvroExporter;
import com.github.susom.starr.dbtoavro.jobs.impl.OracleLoadDatabase;
import com.github.susom.starr.dbtoavro.jobs.impl.SqlServerLoadBackup;
import com.github.susom.starr.dbtoavro.jobs.impl.SqlServerLoadDatabase;
import com.github.susom.starr.dbtoavro.util.DatabaseProviderRx;
import com.github.susom.starr.dbtoavro.entity.LocalDateTimeSerializer;
import java.lang.reflect.Modifier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Master job runner
 */
public class JobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  private final Job job;
  private final Config config;
  private final DatabaseProviderRx.Builder dbb;

  public JobRunner(Config config, Job job) {
    this.config = config;
    this.job = job;
    this.dbb = DatabaseProviderRx
      .pooledBuilder(config)
      .withSqlInExceptionMessages()
      .withConnectionAccess()
      .withSqlParameterLogging();
  }

  /**
   * Entry point for running jobs
   *
   * @return completable
   */
  public Completable run() {

    Loader loader;
    switch (job.flavor) {
      case sqlserver:
        if (job.backupDir != null) {
          loader = new SqlServerLoadBackup(config, dbb);
        } else {
          loader = new SqlServerLoadDatabase(config, dbb);
        }
        break;
      case oracle:
        if (job.backupDir != null) {
          loader = new OracleLoadDataPump(config, dbb);
        } else {
          loader = new OracleLoadDatabase(config, dbb);
        }
        break;
      default:
        return Completable
          .error(new IllegalArgumentException("Unimplemented database " + job.flavor));
    }

    if (job.destination != null) {
      Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.TRANSIENT).setPrettyPrinting()
      .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerializer())
      .create();
      //excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
      long startTime = System.nanoTime();
      return new AvroExporter(config, dbb).run(job, loader)
        .toList()
        .doOnSuccess(avro -> {
          job.avro = avro;
          job.groupedAvro = avro.stream().filter(w -> w.tableName != null).collect(Collectors.groupingBy(w -> w.tableName));
          job.runtimeMs = (System.nanoTime() - startTime) / 1000000;
          Path output = Paths.get(job.logfile);
          Files.write(output, gson.toJson(job).getBytes(StandardCharsets.UTF_8));
          LOGGER.info("Wrote {}", job.logfile);
          for (AvroFile avroFile : job.avro) {
            if(avroFile.table != null)
            {
              if (avroFile.table.getColumns().stream().anyMatch(c -> !c.supported)) {
                LOGGER.warn(String.format(Locale.ROOT, "Table %s had unsupported columns [%s]",
                  avroFile.table.getName(), avroFile.table.getColumns().stream()
                    .filter(c -> !c.supported)
                    .map(c -> c.name + " (" + c.vendorType + "," + c.jdbcType + ")")
                    .collect(Collectors.joining(", "))));
              }
            }
          }
        })
        .ignoreElement();
    } else {
      LOGGER.info("No destination, not exporting avro");
      return loader.run(job).ignoreElement().andThen(loader.stop());
    }

  }

}
