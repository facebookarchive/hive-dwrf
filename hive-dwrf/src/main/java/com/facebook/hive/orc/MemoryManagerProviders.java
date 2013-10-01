package com.facebook.hive.orc;

import org.apache.hadoop.conf.Configuration;

public class MemoryManagerProviders {
  private static MemoryManager STATIC_MEMORY_MANAGER = null;

  public static MemoryManagerProvider staticSingleton() {
    return new MemoryManagerProvider()
    {
      @Override
      public MemoryManager get(Configuration conf)
      {
        synchronized (MemoryManagerProviders.class) {
          if (STATIC_MEMORY_MANAGER == null) {
            STATIC_MEMORY_MANAGER = new MemoryManager(conf);
          }
          return STATIC_MEMORY_MANAGER;
        }
      }
    };
  }

  public static MemoryManagerProvider fromInstance(final MemoryManager memoryManager) {
    return new MemoryManagerProvider()
    {
        @Override
        public MemoryManager get(Configuration conf)
        {
            return memoryManager;
        }
    };
  }
}
