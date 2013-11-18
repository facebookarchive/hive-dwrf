package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;

public class OrcLazyTimestamp extends OrcLazyObject {

  public OrcLazyTimestamp(LazyTimestampTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyTimestamp(OrcLazyTimestamp copy) {
    super(copy);
    previous = new TimestampWritable(((TimestampWritable)copy.previous));
  }
}
