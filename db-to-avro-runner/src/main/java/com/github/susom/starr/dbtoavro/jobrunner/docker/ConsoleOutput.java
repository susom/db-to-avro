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

package com.github.susom.starr.dbtoavro.jobrunner.docker;

import com.github.dockerjava.api.model.StreamType;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for retrieving Streams from docker API
 */
public class ConsoleOutput {

  private static Pattern percentPattern = Pattern.compile("(\\d+)\\s*(%\\s*|percent)");
  private StreamType streamType;
  private String line;

  public ConsoleOutput(StreamType streamType, String line) {
    this.streamType = streamType;
    this.line = line;
  }

  public StreamType getStreamType() {
    return streamType;
  }

  public String getData() {
    return line;
  }

  /**
   * Looks for 'xx percent' in the current line, returns that value, otherwise -1. Useful for following the files
   * of long-running commands that print "xx percent complete"
   *
   * @return percent value found in the string
   */
  public int getPercent() {
    int value = -1;
    try {
      Matcher matcher = percentPattern.matcher(line);
      while (matcher.find()) {
        value = Integer.valueOf(matcher.group(1));
      }
    } catch (Exception ignored) {
      // expected
    }
    return value;
  }

}