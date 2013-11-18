package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.io.ByteWritable;

public class OrcLazyByte extends OrcLazyObject {
  public OrcLazyByte(LazyByteTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyByte(OrcLazyByte copy) {
    super(copy);
    previous = new ByteWritable(((ByteWritable)copy.previous).get());
  }
}
