/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.util;

import java.util.concurrent.TimeUnit;

public class TimingUtil {

  /**
   * Calculate the elapsed time between two times specified in milliseconds.
   * 
   * @param start
   *          The start of the time period
   * @param end
   *          The end of the time period
   * @return a string of the form "XhYmZs" when the elapsed time is X hours, Y
   *         minutes and Z seconds or null if start > end.
   */
  public static String elapsedTime(long start, long end) {
    if (start > end) {
      return null;
    }
    return secondsToHMS((end-start)/1000);
  }
  
  /**
   * Show time in seconds as hours, minutes and seconds (hh:mm:ss)
   * 
   * @param seconds
   *          (elapsed) time in seconds
   * @return human readable time string "hh:mm:ss"
   */
  public static String secondsToHMS(long seconds) {
    long hours = TimeUnit.SECONDS.toHours(seconds);
    long minutes = TimeUnit.SECONDS.toMinutes(seconds)
        % TimeUnit.HOURS.toMinutes(1);
    seconds = TimeUnit.SECONDS.toSeconds(seconds)
        % TimeUnit.MINUTES.toSeconds(1);
    return String.format("%02d:%02d:%02d", hours, minutes, seconds);
  }

  /**
   * Show time in seconds as days, hours, minutes and seconds (d days, hh:mm:ss)
   * 
   * @param seconds
   *          (elapsed) time in seconds
   * @return human readable time string "d days, hh:mm:ss"
   */
  public static String secondsToDaysHMS(long seconds) {
    long days = TimeUnit.SECONDS.toDays(seconds);
    if (days == 0)
      return secondsToHMS(seconds);
    String hhmmss = secondsToHMS(seconds % TimeUnit.DAYS.toSeconds(1));
    return String.format("%d days, %s", days, hhmmss);
  }

}
