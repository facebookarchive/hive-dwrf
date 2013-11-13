package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.io.IntWritable;

public class OrcLazyInt extends OrcLazyObject {

  public OrcLazyInt(LazyIntTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyInt(OrcLazyInt copy) {
    super(copy);
    previous = new IntWritable(((IntWritable)copy.previous).get());
  }
}
