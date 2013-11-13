package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class OrcLazyDoubleObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyDouble, DoubleWritable>
    implements DoubleObjectInspector {

  protected OrcLazyDoubleObjectInspector() {
    super(PrimitiveObjectInspectorUtils.doubleTypeEntry);
  }

  public double get(Object o) {
    return ((DoubleWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyDouble((OrcLazyDouble) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    DoubleWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : Double.valueOf(writable.get());
  }
}
