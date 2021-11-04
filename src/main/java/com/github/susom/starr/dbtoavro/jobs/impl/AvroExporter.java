package com.github.susom.starr.dbtoavro.jobs.impl;

import com.github.susom.database.Config;
import com.github.susom.starr.dbtoavro.entity.AvroFile;
import com.github.susom.starr.dbtoavro.entity.Job;
import com.github.susom.starr.dbtoavro.functions.AvroFns;
import com.github.susom.starr.dbtoavro.functions.DatabaseFns;
import com.github.susom.starr.dbtoavro.functions.impl.FnFactory;
import com.github.susom.starr.dbtoavro.jobs.Exporter;
import com.github.susom.starr.dbtoavro.jobs.Loader;
import com.github.susom.database.DatabaseProvider;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroExporter implements Exporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroExporter.class);

  private final Config config;
  private DatabaseProvider.Builder dbb;

  public AvroExporter(Config config, DatabaseProvider.Builder dbb) {
    this.config = config;
    this.dbb = dbb;
  }

  @Override
  public Observable<AvroFile> run(Job job, Loader loader) {

    int threads = config.getIntegerOrThrow("threads");
    ExecutorService writerPool = Executors.newFixedThreadPool(threads);
    ExecutorService metadataPool = Executors.newFixedThreadPool(threads);
    LOGGER.info("Starting export using {} threads", threads);
    final int maxRetryCount = 4;
    final int delay = 5;
    return loader.run(job)
      .flatMapObservable(database -> {
        AvroFns avroFns = FnFactory.getAvroFns(database.flavor, job, dbb);
        DatabaseFns dbFns = FnFactory.getDatabaseFns(database.flavor, config, dbb);
        return
          dbFns.getSchemas(job.catalog)
              .filter(schema ->
              job.schemas.isEmpty()
                || job.schemas.stream().anyMatch(x -> x.equals(schema)))
            .flatMap(schema ->
              dbFns.getTables(schema, job)
                .filter(table ->
                  job.tables.isEmpty()
                    || job.tables.stream().anyMatch(x -> x.equals(schema + "." + table))
                )
                .filter(table ->
                  job.tableExclusions.isEmpty()
                    || job.tableExclusions.stream()
                    .noneMatch(re -> (schema + "." + table).matches("(?i:" + re + ")"))
                )
                .flatMap(tableName ->
                  dbFns.getQueries(schema, tableName, job)
                  .flatMap(query -> avroFns.saveAsAvro(query)
                    .subscribeOn(Schedulers.from(writerPool))
                    .toObservable()
                    //.onErrorReturnItem(new AvroFile(query, false))
                    .retryWhen(errors -> //this retry is for saveAsAvro
                      errors
                            .zipWith(Observable.range(1, maxRetryCount), (error, retryCount) -> retryCount)
                            .flatMap(retryCount -> Observable.timer((long) Math.pow(delay, retryCount), TimeUnit.SECONDS, Schedulers.from(writerPool)) )
                      )
                  ,false, threads) // don't create more writer observables than #threads
                  .subscribeOn(Schedulers.from(metadataPool))
                  .retryWhen(errors -> //this retry is for getQueries
                    errors
                          .zipWith(Observable.range(1, maxRetryCount), (error, retryCount) -> retryCount)
                          .flatMap(retryCount -> Observable.timer((long) Math.pow(delay, retryCount), TimeUnit.SECONDS, Schedulers.from(writerPool)))
                   )
                ,false, threads) // only look ahead by #threads tables
            );
        }
      )
      .doOnComplete(writerPool::shutdown);
  }

}
