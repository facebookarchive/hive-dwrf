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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;

// This class cannot depend on any variables HiveConf not in open source because ORC depends
// on it, and Presto uses open source Hive.
public class ReaderWriterProfiler {
  public static enum Counter {
    DECOMPRESSION_TIME(0, Type.READ),
    COMPRESSION_TIME(1, Type.WRITE),
    SERIALIZATION_TIME(2, Type.WRITE),
    DESERIALIZATION_TIME(3, Type.READ),
    DECODING_TIME(4, Type.READ),
    ENCODING_TIME(5, Type.WRITE);

    public static enum Type {
      READ,
      WRITE;
    }

    private int value;
    private Type type;
    private Counter(int v, Type type) {
      value = v;
      this.type = type;
    }
  }
  public static enum ReadWriteCounter {
    READ_TIME,
    WRITE_TIME
  }

  // These constants also appear in HiveConf, be sure to update that file as well with
  // any changes here.
  private static final String HIVE_READER_WRITER_PROFILER_ENABLED_CONFIG =
    "hive.exec.profiler.readwrite";
  private static final boolean HIVE_READER_WRITER_PROFILER_ENABLED_DEFAULT = false;
  private static final String HIVE_READER_WRITER_PROFILER_USE_CPU_CONFIG =
    "hive.exec.profiler.readwrite.cpu";
  private static final boolean HIVE_READER_WRITER_PROFILER_USE_CPU_DEFAULT = false;

  private static boolean profile = false;
  private static boolean useCpuTime = false;
  private static ReaderWriterProfiler instance;

  protected static long [] profileStart = new long[6];
  protected static int [] profileStarted = new int[6];
  protected static int [] ended = new int[6];
  protected static int [] started = new int[6];
  protected static long [] profileTimes = new long[6];
  protected static long [] profileTypeStart = new long[2];
  protected static int [] profileTypeStarted = new int[2];
  protected static int [] typeEnded = new int[2];
  protected static int [] typeStarted = new int[2];
  protected static long [] profileTypeTimes = new long[2];
  public static final Log LOG = LogFactory
      .getLog(ReaderWriterProfiler.class.getName());


  private ReaderWriterProfiler() {}

  public static void setProfilerOptions(Configuration conf) {
    if (conf != null) {
      if(conf.getBoolean(HIVE_READER_WRITER_PROFILER_ENABLED_CONFIG,
          HIVE_READER_WRITER_PROFILER_ENABLED_DEFAULT)) {

        profile = true;
        useCpuTime = conf.getBoolean(HIVE_READER_WRITER_PROFILER_USE_CPU_CONFIG,
            HIVE_READER_WRITER_PROFILER_USE_CPU_DEFAULT);
      }
      instance = createInstance();
    }
    if (instance == null) {
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

    long read = profileTypeTimes[Counter.Type.READ.ordinal()];
    long write = profileTypeTimes[Counter.Type.WRITE.ordinal()];
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
      long cpuTime = -1;
      if (profileStarted[c.value] == 0) {
        started[c.value] += 1;
        cpuTime = getCpuTime(mainThreadId);
        profileStart[c.value] = cpuTime;
      }
      if (profileTypeStarted[c.type.ordinal()] == 0) {
        typeStarted[c.type.ordinal()] += 1;
        if (cpuTime == -1) {
          cpuTime = getCpuTime(mainThreadId);
        }
        profileTypeStart[c.type.ordinal()] = cpuTime;
      }
      profileStarted[c.value] += 1;
      profileTypeStarted[c.type.ordinal()] += 1;
    }

    @Override
    protected void endProfiler(Counter c) {
      profileStarted[c.value] -= 1;
      profileTypeStarted[c.type.ordinal()] -= 1;
      long cpuTime = -1;
      if (profileStarted[c.value] == 0) {
        cpuTime = getCpuTime(mainThreadId);
        profileTimes[c.value] += cpuTime - profileStart[c.value];
        ended[c.value] += 1;
      }
      if (profileTypeStarted[c.type.ordinal()] == 0) {
        if (cpuTime == -1) {
          cpuTime = getCpuTime(mainThreadId);
        }
        profileTypeTimes[c.type.ordinal()] += cpuTime - profileTypeStart[c.type.ordinal()];
        typeEnded[c.type.ordinal()] += 1;
      }
    }
  }

  private static class WalltimeReaderWriterProfiler extends ReaderWriterProfiler {

    @Override
    protected void startProfiler(Counter c) {

      long time = -1;
      if (profileStarted[c.value] == 0) {
        started[c.value] += 1;
        time = System.nanoTime();
        profileStart[c.value] = time;
      }
      if (profileTypeStarted[c.type.ordinal()] == 0) {
        typeStarted[c.type.ordinal()] += 1;
        if (time == -1) {
          time = System.nanoTime();
        }
        profileTypeStart[c.type.ordinal()] = time;
      }
      profileStarted[c.value] += 1;
      profileTypeStarted[c.type.ordinal()] += 1;
    }

    @Override
    protected void endProfiler(Counter c) {
      profileStarted[c.value] -= 1;
      profileTypeStarted[c.type.ordinal()] -= 1;
      long time = -1;
      if (profileStarted[c.value] == 0) {
        time = System.nanoTime();
        profileTimes[c.value] += time - profileStart[c.value];
        ended[c.value] += 1;
      }
      if (profileTypeStarted[c.type.ordinal()] == 0) {
        if (time == -1) {
          time = System.nanoTime();
        }
        profileTypeTimes[c.type.ordinal()] += time - profileTypeStart[c.type.ordinal()];
        typeEnded[c.type.ordinal()] += 1;
      }
    }
  }
}
