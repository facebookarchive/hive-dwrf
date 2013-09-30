package com.facebook.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;

public interface MemoryManagerProvider {
  MemoryManager get(Configuration conf);
}
