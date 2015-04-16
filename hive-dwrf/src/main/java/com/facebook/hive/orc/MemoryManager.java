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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Implements a memory manager that keeps a global context of how many ORC
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 *
 * This class is thread safe and uses synchronization around the shared state
 * to prevent race conditions.
 */
class MemoryManager {

  private static final Log LOG = LogFactory.getLog(MemoryManager.class);

  /**
   * How often should we check the memory sizes? Measured in rows added
   * to all of the writers.
   */
  private static final int ROWS_BETWEEN_CHECKS = 5000;
  private final long totalMemoryPool;
  protected final Map<Path, WriterInfo> writerList =
      new ConcurrentHashMap<Path, WriterInfo>();
  private long totalAllocation = 0;
  private double currentScale = 1;
  private int rowsAddedSinceCheck = 0;
  // Indicates whether or not there are so many writer instances/columns that the amount of
  // memory one needs to start up, exceeds the amount of memory allocated.
  private boolean lowMemoryMode = false;
  private final long minAllocation;

  protected static class WriterInfo {
    long allocation;
    Callback callback;
    // Count to indicate the number of times this writer was flushed since the last time memory
    // was checked
    int flushedCount = 0;
    // This value is multiplied by the allocation to get an allocation adjusted based on how often
    // this writer gets flushed
    double allocationMultiplier = 1;
    WriterInfo(long allocation, Callback callback) {
      this.allocation = allocation;
      this.callback = callback;
    }
  }

  public interface Callback {
    /**
     * If the initial amount of memory needed by a writer is greater than the amount allocated,
     * call this to try to get the writers to use less memory to avoid an OOM
     * @return
     * @throws IOException
     */
    void enterLowMemoryMode() throws IOException;
  }

  /**
   * Create the memory manager.
   * @param conf use the configuration to find the maximum size of the memory
   *             pool.
   */
  MemoryManager(Configuration conf) {
    double maxLoad = OrcConf.getFloatVar(conf, OrcConf.ConfVars.HIVE_ORC_FILE_MEMORY_POOL);
    totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * maxLoad);
    minAllocation = OrcConf.getLongVar(conf, OrcConf.ConfVars.HIVE_ORC_FILE_MIN_MEMORY_ALLOCATION);
    lowMemoryMode = OrcConf.getBoolVar(conf,
        OrcConf.ConfVars.HIVE_ORC_FILE_ENABLE_LOW_MEMORY_MODE);
  }

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   * @param initialAllocation the current size of the buffer
   */
  synchronized void addWriter(Path path, long requestedAllocation,
                              Callback callback, long initialAllocation) throws IOException {
    WriterInfo oldVal = writerList.get(path);
    // this should always be null, but we handle the case where the memory
    // manager wasn't told that a writer wasn't still in use and the task
    // starts writing to the same path.
    if (oldVal == null) {
      LOG.info("Registering writer for path " + path.toString());
      oldVal = new WriterInfo(requestedAllocation, callback);
      writerList.put(path, oldVal);
      totalAllocation += requestedAllocation;
    } else {
      // handle a new writer that is writing to the same path
      totalAllocation += requestedAllocation - oldVal.allocation;
      oldVal.allocation = requestedAllocation;
      oldVal.callback = callback;
    }
    updateScale(true);

    // If we're not already in low memory mode, and the initial allocation already exceeds the
    // allocation, enter low memory mode to try to avoid an OOM
    if (!lowMemoryMode && requestedAllocation * currentScale <= initialAllocation) {
      lowMemoryMode = true;
      LOG.info("ORC: Switching to low memory mode");
      for (WriterInfo writer : writerList.values()) {
        writer.callback.enterLowMemoryMode();
      }
    }
  }

  public synchronized boolean isLowMemoryMode() {
    return lowMemoryMode;
  }

  /**
   * Remove the given writer from the pool.
   * @param path the file that has been closed
   */
  synchronized void removeWriter(Path path) throws IOException {
    WriterInfo val = writerList.get(path);
    if (val != null) {
      LOG.info("Unregeristering writer for path " + path.toString());
      writerList.remove(path);
      totalAllocation -= val.allocation;
      updateScale(false);
    }
  }

  /**
   * Get the total pool size that is available for ORC writers.
   * @return the number of bytes in the pool
   */
  long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * The scaling factor for each allocation to ensure that the pool isn't
   * oversubscribed.
   * @return a fraction between 0.0 and 1.0 of the requested size that is
   * available for each writer.
   */
  synchronized double getAllocationScale() {
    return currentScale;
  }

  /**
   * Give the memory manager an opportunity for doing a memory check.
   * @throws IOException
   */
  synchronized void addedRow() throws IOException {
    if (++rowsAddedSinceCheck >= ROWS_BETWEEN_CHECKS) {
      notifyWriters();
    }
  }

  synchronized boolean shouldFlush(MemoryEstimate memoryEstimate, Path path, long stripeSize, long maxDictSize)
      throws IOException {

    WriterInfo writer = writerList.get(path);
    if (writer == null) {
      throw new IOException("No writer registered for path " + path.toString());
    }
    long limit = Math.round(stripeSize * currentScale * writer.allocationMultiplier);
    if (memoryEstimate.getTotalMemory() > limit
        || (maxDictSize > 0 && memoryEstimate.getDictionaryMemory() > maxDictSize)) {
      writer.flushedCount++;
      return true;
    }

    return false;
  }

  // A list of writers to share allocations taken from writers which don't need them
  private final List<WriterInfo> writersForAllocation = new ArrayList<WriterInfo>();
  // A list of writers to take allocations from and give to more needy writers
  private final List<WriterInfo> writersForDeallocation = new ArrayList<WriterInfo>();

  /**
   * Notify all of the writers that they should check their memory usage.
   * @throws IOException
   */
  private synchronized void notifyWriters() throws IOException {
    LOG.debug("Notifying writers after " + rowsAddedSinceCheck);
    for(WriterInfo writer: writerList.values()) {
      if (lowMemoryMode) {
        // If we're in low memory mode and a writer
        // 1) flushes twice in a row, mark it as needy
        // 2) doesn't flush twice in a row, mark it for a decreased allocation
        if (writer.flushedCount == 0) {
          writersForDeallocation.add(writer);
        } else if (writer.flushedCount >= 2){
          writersForAllocation.add(writer);
        }
        writer.flushedCount = 0;
      }
    }

    if (lowMemoryMode) {
      reallocateMemory(writersForAllocation, writersForDeallocation);
      writersForDeallocation.clear();
      writersForAllocation.clear();
    }

    rowsAddedSinceCheck = 0;
  }

  private void reallocateMemory(List<WriterInfo> writersForAllocation, List<WriterInfo> writersForDeallocation) {
    if (writersForDeallocation.size() > 0 && writersForAllocation.size() > 0) {
      // If there were some needy writers and some writers that could spare some allocation,
      // redistribute the memory from the bourgeoisie to the proletariat
      double memoryToReallocate = 0;
      for (WriterInfo writer : writersForDeallocation) {
        // Divide the allocation of rich writers which can spare by 2
        double newAllocationMultiplier = writer.allocationMultiplier / 2;
        double reallocation = writer.allocation * currentScale * newAllocationMultiplier;

        // Only take the allocation if the writer will still have at least the minimum allocation
        if (reallocation >= minAllocation) {
          writer.allocationMultiplier /= 2;
          memoryToReallocate += reallocation;
        }
      }

      double memoryToReallocatePerWriter = memoryToReallocate / writersForAllocation.size();
      for (WriterInfo writer : writersForAllocation) {
        // Reallocate the spoils
        // This is just some algebra
        // (allocation * currentScale * multiplier) + reallocation = (allocation * currentScale * X)
        // where X is the new multiplier
        writer.allocationMultiplier =
          ((writer.allocation * currentScale * writer.allocationMultiplier)
              + memoryToReallocatePerWriter) / (writer.allocation * currentScale);
      }
    }
  }

  /**
   * Update the currentScale based on the current allocation and pool size.
   * This also updates the notificationTrigger.
   * @param isAllocate is this an allocation?
   */
  private void updateScale(boolean isAllocate) throws IOException {
    if (totalAllocation <= totalMemoryPool) {
      currentScale = 1;
    } else {
      currentScale = (double) totalMemoryPool / totalAllocation;
    }
  }
}
