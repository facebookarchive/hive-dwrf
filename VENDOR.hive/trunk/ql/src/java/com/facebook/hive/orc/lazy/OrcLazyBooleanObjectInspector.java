package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BooleanWritable;

public class OrcLazyBooleanObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyBoolean, BooleanWritable> implements
    BooleanObjectInspector {

  protected OrcLazyBooleanObjectInspector() {
    super(PrimitiveObjectInspectorUtils.booleanTypeEntry);
  }

  @Override
  public boolean get(Object o) {
    return ((BooleanWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyBoolean((OrcLazyBoolean) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    BooleanWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : Boolean.valueOf(writable.get());
  }
}
