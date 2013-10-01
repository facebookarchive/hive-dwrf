package com.facebook.hive.orc;

import org.apache.hadoop.conf.Configuration;

public interface MemoryManagerProvider {
  MemoryManager get(Configuration conf);
}
