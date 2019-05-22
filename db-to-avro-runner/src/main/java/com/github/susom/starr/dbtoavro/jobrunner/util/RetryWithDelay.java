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

package com.github.susom.starr.dbtoavro.jobrunner.util;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Publisher;

/**
 * Helper class which will retry a completable for a given number of times, with optional waiting period between
 * retries.
 */
public class RetryWithDelay implements Function<Flowable<Throwable>, Publisher<?>> {

  private final int maxRetries;
  private final long retryDelayMillis;
  private int retryCount;

  public RetryWithDelay(final int maxRetries, final int retryDelayMillis) {
    this.maxRetries = maxRetries;
    this.retryDelayMillis = retryDelayMillis;
    this.retryCount = 1;
  }

  @Override
  public Publisher<?> apply(Flowable<Throwable> throwableFlowable) {
    return throwableFlowable.flatMap((Function<Throwable, Publisher<?>>) throwable -> {
      if (retryCount++ <= maxRetries) {
        // When this Observable calls onNext, the original
        // Observable will be retried (i.e. re-subscribed).
        return Flowable.timer(retryDelayMillis,
            TimeUnit.MILLISECONDS);
      }
      // Max retries hit. Just pass the error along.
      return Flowable.error(throwable);
    });
  }
}