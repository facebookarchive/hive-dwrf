/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
