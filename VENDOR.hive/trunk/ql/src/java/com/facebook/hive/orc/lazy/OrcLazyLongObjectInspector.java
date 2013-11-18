package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

public class OrcLazyLongObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyLong, LongWritable>
    implements LongObjectInspector {

  protected OrcLazyLongObjectInspector() {
    super(PrimitiveObjectInspectorUtils.longTypeEntry);
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyLong((OrcLazyLong) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    LongWritable writable = (LongWritable) getPrimitiveWritableObject(o);
    return writable == null ? null : Long.valueOf(writable.get());
  }

  @Override
  public long get(Object o) {
    return getPrimitiveWritableObject(o).get();
  }

}
