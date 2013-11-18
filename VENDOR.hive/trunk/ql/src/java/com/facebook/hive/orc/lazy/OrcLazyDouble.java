package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class OrcLazyDouble extends OrcLazyObject {

  public OrcLazyDouble(LazyDoubleTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyDouble(OrcLazyDouble copy) {
    super(copy);
    previous = new DoubleWritable(((DoubleWritable)copy.previous).get());
  }
}
