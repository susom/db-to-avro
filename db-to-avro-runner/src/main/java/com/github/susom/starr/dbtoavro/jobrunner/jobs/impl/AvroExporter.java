package com.github.susom.starr.dbtoavro.jobrunner.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.jobrunner.entity.AvroFile;
import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.starr.dbtoavro.jobrunner.functions.AvroFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.jobrunner.functions.impl.FnFactory;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Exporter;
import com.github.susom.starr.dbtoavro.jobrunner.jobs.Loader;
import com.github.susom.starr.dbtoavro.jobrunner.util.DatabaseProviderRx;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroExporter implements Exporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExporter.class);

  private final Config config;
  private final String filePattern;
  private final long threshold;
  private final long bytes;
  private final int cores;
  private DatabaseProviderRx.Builder dbb;

  public AvroExporter(Config config, DatabaseProviderRx.Builder dbb) {
    this.config = config;
    this.dbb = dbb;
    this.threshold = config.getLong("avro.split.threshold", 1000000000);
    this.bytes = config.getLong("avro.split.bytes", 250000000);
    this.filePattern = config.getString("avro.filename", "%{SCHEMA}.%{TABLE}-%{PART}.avro");
    this.cores = Runtime.getRuntime().availableProcessors();
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {

    long targetSize = config.getLong("avro.targetsize", 0);
    int threads = (int) (cores * (config.getDouble("avro.core.multiplier", 0.5)));
    String path = job.destination + filePattern;

    ExecutorService dbPoolSched = Executors.newFixedThreadPool(threads);

    LOGGER.info("Starting export using {} threads", threads);

    return loader.run(job)
        .flatMapObservable(database -> {
          AvroFns avroFns = FnFactory.getAvroFns(database.flavor, config, dbb);
          DatabaseFns dbFns = FnFactory.getDatabaseFns(database.flavor, config, dbb);
          if (avroFns == null || dbFns == null) {
            return Observable.error(new Exception("Unsupported database flavor!"));
          }

          return dbFns.getCatalogs(database)
              .filter(job.catalog::equals) // filter out unwanted databases
              .flatMap(catalog ->

                  dbFns.getSchemas(catalog)
                      .filter(job.schemas::contains) // filter out unwanted schemas
                      .flatMap(schema ->

                          dbFns.getTables(catalog, schema)
                              .filter(t -> job.tables.isEmpty() || job.tables.contains(t.name))
                              .toFlowable(BackpressureStrategy.BUFFER)
                              .parallel()
                              .runOn(Schedulers.from(dbPoolSched))
                              .flatMap(table ->

                                  dbFns.introspect(table)
                                      .andThen(avroFns.getPartitions(table, targetSize)
                                          .flatMap(avroFns::savePartitionAsAvro)
                                          .switchIfEmpty(
                                              avroFns.saveTableAsAvro(table, path, targetSize)
                                          )
                                      ).toFlowable(BackpressureStrategy.BUFFER)

                              )
                              .sequential()
                              .toObservable()

                      )

              );
        })
        .doOnComplete(dbPoolSched::shutdown);
  }

}
