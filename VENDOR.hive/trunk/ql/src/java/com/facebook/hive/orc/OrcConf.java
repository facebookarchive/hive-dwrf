//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.

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

package com.facebook.hive.orc;

import org.apache.hadoop.conf.Configuration;

// All configs in this class also appear in HiveConf, so any changes here should also be made there
// This is because only HiveConf can provide type checking through the CLI, and Presto depends on
// open source Hive, and so won't work with any variables not in open source HiveConf
public class OrcConf {

  public static enum ConfVars {
    HIVE_ORC_COMPRESSION("hive.exec.orc.compress", "ZLIB"),
    HIVE_ORC_ZLIB_COMPRESSION_LEVEL("hive.exec.orc.compress.zlib.level", 4),
    HIVE_ORC_COMPRESSION_BLOCK_SIZE("hive.exec.orc.compress.size", 262144),
    HIVE_ORC_STRIPE_SIZE("hive.exec.orc.stripe.size", 268435456L),
    HIVE_ORC_ROW_INDEX_STRIDE("hive.exec.orc.row.index.stride", 10000),
    HIVE_ORC_CREATE_INDEX("hive.exec.orc.create.index", true),

    HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD(
        "hive.exec.orc.dictionary.key.numeric.size.threshold", 0.7f),
    HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD(
        "hive.exec.orc.dictionary.key.string.size.threshold", 0.8f),
    HIVE_ORC_DICTIONARY_SORT_KEYS("hive.exec.orc.dictionary.key.sorted", true),
    HIVE_ORC_BUILD_STRIDE_DICTIONARY("hive.exec.orc.build.stride.dictionary", true),

    HIVE_ORC_ENTROPY_KEY_STRING_SIZE_THRESHOLD(
        "hive.exec.orc.entropy.key.string.size.threshold", 0.9f),
    HIVE_ORC_ENTROPY_STRING_MIN_SAMPLES("hive.exec.orc.entropy.string.min.samples", 100),
    HIVE_ORC_ENTROPY_STRING_DICT_SAMPLE_FRACTION(
        "hive.exec.orc.entropy.string.dict.sample.fraction", 0.001f),
    HIVE_ORC_ENTROPY_STRING_THRESHOLD("hive.exec.orc.entropy.string.threshold", 20),

    HIVE_ORC_DICTIONARY_ENCODING_INTERVAL("hive.exec.orc.encoding.interval", 30),
    HIVE_ORC_USE_VINTS("hive.exec.orc.use.vints", true),
    HIVE_ORC_READ_COMPRESSION_STRIDES("hive.orc.read.compression.strides", 5),

    // Maximum fraction of heap that can be used by ORC file writers
    HIVE_ORC_FILE_MEMORY_POOL("hive.exec.orc.memory.pool", 0.5f), // 50%
    HIVE_ORC_FILE_MIN_MEMORY_ALLOCATION("hive.exec.orc.min.mem.allocation", 4194304L), // 4 Mb
    HIVE_ORC_FILE_ENABLE_LOW_MEMORY_MODE("hive.exec.orc.low.memory", false),
    HIVE_ORC_ROW_BUFFER_SIZE("hive.exec.orc.row.buffer.size", 100),

    HIVE_ORC_EAGER_HDFS_READ("hive.exec.orc.eager.hdfs.read", true),
    HIVE_ORC_EAGER_HDFS_READ_BYTES("hive.exec.orc.eager.hdfs.read.bytes", 193986560), // 185 Mb
    ;

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final boolean defaultBoolVal;


    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.defaultVal = Integer.toString(defaultIntVal);
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.defaultVal = Long.toString(defaultLongVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.defaultVal = Float.toString(defaultFloatVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.defaultVal = Boolean.toString(defaultBoolVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    conf.setInt(var.varname, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    return conf.getLong(var.varname, var.defaultLongVal);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    conf.setLong(var.varname, val);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public static void setFloatVar(Configuration conf, ConfVars var, float val) {
    conf.setFloat(var.varname, val);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    conf.setBoolean(var.varname, val);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    return conf.get(var.varname, var.defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    conf.set(var.varname, val);
  }
}
