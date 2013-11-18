package com.facebook.hive.orc.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;

public class OrcLazyFloatObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyFloat, FloatWritable> implements
    FloatObjectInspector {

  OrcLazyFloatObjectInspector() {
    super(PrimitiveObjectInspectorUtils.floatTypeEntry);
  }

  public float get(Object o) {
    return ((FloatWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyFloat((OrcLazyFloat) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    FloatWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : Float.valueOf(writable.get());
  }
}
