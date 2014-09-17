package com.facebook.hive.orc;

/**
 *
 * MemoryEstimate.
 *
 * A global entity to keep track of memory used across all WriterImpls.
 *
 * The values represent estimates of the amount of memory used by the writers.
 *
 */
public class MemoryEstimate {
  // The estimate of the total amount of memory currently being used
  private long totalMemory;
  // The memory being used by the dictionaries (note this should only account for memory
  // actually being used, not memory allocated for potential use in the future)
  private long dictionaryMemory;

  public long getTotalMemory() {
    return totalMemory;
  }

  public long getDictionaryMemory() {
    return dictionaryMemory;
  }

  public void incrementTotalMemory(long increment) {
    totalMemory += increment;
  }

  public void incrementDictionaryMemory(long increment) {
    dictionaryMemory += increment;
  }

  public void decrementTotalMemory(long decrement) { totalMemory -= decrement; }

  public void decrementDictionaryMemory(long decrement) { dictionaryMemory -= decrement; }

  public void reset() {
    totalMemory = 0;
    dictionaryMemory = 0;
  }
}
