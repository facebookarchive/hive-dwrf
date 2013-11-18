package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.IntWritable;

public class OrcLazyIntObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyInt, IntWritable> implements
    IntObjectInspector {

  protected OrcLazyIntObjectInspector() {
    super(PrimitiveObjectInspectorUtils.intTypeEntry);
  }

  @Override
  public int get(Object o) {
    return ((IntWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyInt((OrcLazyInt) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    IntWritable writable = (IntWritable) getPrimitiveWritableObject(o);
    return writable == null ? null : Integer.valueOf(writable.get());
  }

}
