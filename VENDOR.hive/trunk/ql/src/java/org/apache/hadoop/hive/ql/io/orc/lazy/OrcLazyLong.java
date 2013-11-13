package org.apache.hadoop.hive.ql.io.orc.lazy;

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
