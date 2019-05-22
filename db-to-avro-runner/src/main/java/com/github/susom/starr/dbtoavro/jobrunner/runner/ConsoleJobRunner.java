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

package com.github.susom.starr.dbtoavro.jobrunner.runner;

import com.github.susom.starr.dbtoavro.jobrunner.entity.Job;
import com.github.susom.database.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job runnner which logs to the console
 */
public class ConsoleJobRunner extends JobRunner implements JobLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);

  public ConsoleJobRunner(Config config, Job job) {
    super(config, job);
  }

  @Override
  public void log(String message) {
    LOGGER.info(message);
  }

  @Override
  public void progress(int complete, int total) {
    int n = Math.min(100, complete * 100 / total);
    LOGGER.info("Progress: {}%", n);
  }

}
