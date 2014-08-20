package com.facebook.hive.orc;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 *
 * MemoryManagerWithForce.
 *
 * An implementation of MemoryManager with the ability to force writers to flush their stripes
 * and to enter low memory mode.
 */
public class MemoryManagerWithForce extends MemoryManager {

  MemoryManagerWithForce(Configuration conf) {
    super(conf);
  }

  public void forceFlushStripe() throws IOException {
    for (WriterInfo writer : writerList.values()) {
      writer.callback.checkMemory(0);
    }
  }

  public void forceEnterLowMemoryMode() throws IOException {
    for (WriterInfo writer : writerList.values()) {
      writer.callback.enterLowMemoryMode();
    }
  }
}
