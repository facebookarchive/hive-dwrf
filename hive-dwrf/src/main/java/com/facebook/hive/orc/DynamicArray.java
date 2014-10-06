package com.facebook.hive.orc;

import org.apache.hadoop.hive.ql.io.slice.SizeOf;
import org.apache.hadoop.hive.ql.io.slice.Slice;
import org.apache.hadoop.hive.ql.io.slice.Slices;

public class DynamicArray {

  protected Slice data;                    // the real data
  protected int length = 0;                // max set element index +1
  private final MemoryEstimate memoryEstimate;
  private final int literalSize;
  private final int defaultSize;

  protected DynamicArray(int size, MemoryEstimate memoryEstimate, int literalSize,
      int defaultSize) {
    if (size <= 0) {
      throw new IllegalArgumentException("bad chunksize");
    }
    this.memoryEstimate = memoryEstimate;
    this.literalSize = literalSize;
    this.defaultSize = defaultSize;
    setData(Slices.allocate(size));
  }

  protected void setData(Slice newData) {
    memoryEstimate.decrementTotalMemory(data == null ? 0 : data.length());
    data = newData;
    memoryEstimate.incrementTotalMemory(data.length());

  }

  /**
   * Ensure that the given index is valid.
   */
  protected void grow(int index) {
    if ((index * literalSize) + (literalSize - 1) >= data.length()) {
      int newSize = Math.max((index * literalSize) + defaultSize, 2 * data.length());
      Slice newSlice = Slices.allocate(newSize);
      newSlice.setBytes(0, data);
      setData(newSlice);
    }
  }

  /**
   * Get the size of the array.
   * @return the number of bytes in the array
   */
  public int size() {
    return length;
  }

  /**
   * Clear the array to its original pristine state.
   */
  public void clear() {
    length = 0;
    setData(Slices.allocate(defaultSize));
  }

  public long getSizeInBytes() {
    return data.length();
  }

  // A cleanup method that should be called before allowing the object to leave scope
  public void cleanup() {
    memoryEstimate.decrementTotalMemory(data.length());
  }
}
