package com.facebook.hive.orc.lazy;

import org.apache.hadoop.io.BytesWritable;

public class OrcLazyBinary extends OrcLazyObject {

  public OrcLazyBinary(LazyBinaryTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyBinary(OrcLazyBinary copy) {
    super(copy);
    BytesWritable copyPrevious = (BytesWritable) copy.previous;
    byte[] bytes = new byte[copyPrevious.getLength()];
    System.arraycopy(copyPrevious.getBytes(), 0, bytes, 0, copyPrevious.getLength());
    previous = new BytesWritable(bytes);
  }
}
