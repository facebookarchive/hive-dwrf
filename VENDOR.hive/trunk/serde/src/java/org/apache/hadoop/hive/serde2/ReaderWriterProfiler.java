/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Reporter;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.conf.Configuration;

public class ReaderWriterProfiler {
  public static enum Counter {
    DECOMPRESSION_TIME(0),
    COMPRESSION_TIME(1),
    SERIALIZATION_TIME(2),
    DESERIALIZATION_TIME(3),
    DECODING_TIME(4),
    ENCODING_TIME(5);

    private int value;
    private Counter(int v) {
      value = v;
    }
  }
  public static enum ReadWriteCounter {  
    READ_TIME,
    WRITE_TIME
  }

  private static boolean profile = false;
  private static boolean useCpuTime = false;
  private static ReaderWriterProfiler instance;

  protected static long [] profileStart = new long[6];
  protected static int [] profileStarted = new int[6];
  protected static int [] ended = new int[6];
  protected static int [] started = new int[6];
  protected static long [] profileTimes = new long[6]; 
  public static final Log LOG = LogFactory
      .getLog(ReaderWriterProfiler.class.getName());


  private ReaderWriterProfiler() {}

  public static void setProfilerOptions(Configuration conf) {
    if (conf != null) {
      if(HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEREADERWRITERPROFILER, false)) {
        profile = true;
        useCpuTime = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEREADERWRITERPROFILERTYPE, false);
      }
      instance = createInstance();
    }
  }

  private static ReaderWriterProfiler createInstance() {
    if (profile) {
      if (useCpuTime) {
        return new CpuReaderWriterProfiler();
      } else {
        return new WalltimeReaderWriterProfiler();
      }
    }
    else {
      return new ReaderWriterProfiler();
    }
  }

  public static ReaderWriterProfiler getInstance() {
    return instance;
  }

  public static void start(Counter c) {
    instance.startProfiler(c);
  }
  
  public static void end(Counter c) {
    instance.endProfiler(c);
  }

  protected void startProfiler(Counter c) { }

  protected void endProfiler(Counter c) { }

  public static void log() {
    log(null);
  }

  public static void log(Reporter logReporter) {
    for (Counter c : Counter.values()) {
      LOG.info(c + " start (" + started[c.value] + "), end (" + ended[c.value] + "): " +  profileTimes[c.value]);
      if (logReporter != null) {
        logReporter.incrCounter(c, profileTimes[c.value]);
      }
    }
    long read = profileTimes[Counter.DECOMPRESSION_TIME.value] +
      profileTimes[Counter.DESERIALIZATION_TIME.value] +
      profileTimes[Counter.DECODING_TIME.value];
    long write = profileTimes[Counter.COMPRESSION_TIME.value] +
      profileTimes[Counter.SERIALIZATION_TIME.value] +
      profileTimes[Counter.ENCODING_TIME.value];
    if (logReporter != null) {
      LOG.info("read time: " + read);
      LOG.info("write time: " + write);
      logReporter.incrCounter(ReadWriteCounter.READ_TIME, read);
      logReporter.incrCounter(ReadWriteCounter.WRITE_TIME, write);
    } 
  }

  private static class CpuReaderWriterProfiler extends ReaderWriterProfiler {

    private final ThreadMXBean BEAN = ManagementFactory.getThreadMXBean();
    private final long mainThreadId = getMainThreadId();

    private long getMainThreadId() {
      final long[] ids = BEAN.getAllThreadIds( );
      final ThreadInfo[] infos = BEAN.getThreadInfo( ids );
      for (int i = 0; i < infos.length; i++) {
        String threadName = infos[i].getThreadName();
        if(threadName.equals("main")) { return ids[i]; }
      }
      return -1;
    }
    
    private long getCpuTime(long mainThreadId) {
      return BEAN.isCurrentThreadCpuTimeSupported() ? BEAN.getThreadCpuTime(mainThreadId) : 0L;
    }

    public CpuReaderWriterProfiler() {}

    @Override
    protected void startProfiler(Counter c) {
      if (profileStarted[c.value] == 0) {
        started[c.value] += 1;
        profileStart[c.value] = getCpuTime(mainThreadId);
      }
      profileStarted[c.value] += 1;
    }
    
    @Override
    protected void endProfiler(Counter c) {
      profileStarted[c.value] -= 1;
      if (profileStarted[c.value] == 0) {
        long end = 0;
        end = getCpuTime(mainThreadId);
        profileTimes[c.value] += end - profileStart[c.value];
        ended[c.value] += 1;
      }
    }
  }

  private static class WalltimeReaderWriterProfiler extends ReaderWriterProfiler {

    @Override
    protected void startProfiler(Counter c) {

      if (profileStarted[c.value] == 0) {
        started[c.value] += 1;
        profileStart[c.value] = System.nanoTime();
      }
      profileStarted[c.value] += 1;
    }
    
    @Override
    protected void endProfiler(Counter c) {
      profileStarted[c.value] -= 1;
      if (profileStarted[c.value] == 0) {
        long end = 0;
        end = System.nanoTime();
        profileTimes[c.value] += end - profileStart[c.value];
        ended[c.value] += 1;
      }
    }
  }
}
