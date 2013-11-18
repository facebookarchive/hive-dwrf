package com.facebook.hive.orc.lazy;

import org.apache.hadoop.io.BooleanWritable;

public class OrcLazyBoolean extends OrcLazyObject {
  public OrcLazyBoolean(LazyBooleanTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyBoolean(OrcLazyBoolean copy) {
    super(copy);
    previous = new BooleanWritable(((BooleanWritable)copy.previous).get());
  }
}
