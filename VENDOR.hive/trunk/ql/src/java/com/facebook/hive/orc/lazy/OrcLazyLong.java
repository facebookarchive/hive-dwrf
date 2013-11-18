package com.facebook.hive.orc.lazy;

import org.apache.hadoop.io.LongWritable;

public class OrcLazyLong extends OrcLazyObject {

  public OrcLazyLong(LazyLongTreeReader treeReader) {
    super(treeReader);
  }

  public OrcLazyLong(OrcLazyLong copy) {
    super(copy);
    previous = new LongWritable(((LongWritable)copy.previous).get());
  }

}
