package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.hive.serde2.io.ShortWritable;

public class OrcLazyShort extends OrcLazyObject {

  public OrcLazyShort(LazyShortTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyShort(OrcLazyShort copy) {
    super(copy);
    previous = new ShortWritable(((ShortWritable)copy.previous).get());
  }
}
