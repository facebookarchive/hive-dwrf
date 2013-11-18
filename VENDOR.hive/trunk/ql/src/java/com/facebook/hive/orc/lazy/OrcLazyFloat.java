package com.facebook.hive.orc.lazy;

import org.apache.hadoop.io.FloatWritable;

public class OrcLazyFloat extends OrcLazyObject {

  public OrcLazyFloat(LazyFloatTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyFloat(OrcLazyFloat copy) {
    super(copy);
    previous = new FloatWritable(((FloatWritable)copy.previous).get());
  }
}
