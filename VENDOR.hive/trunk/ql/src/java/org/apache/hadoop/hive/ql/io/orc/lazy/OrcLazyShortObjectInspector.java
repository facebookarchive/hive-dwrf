package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;

public class OrcLazyShortObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyShort, ShortWritable>
    implements ShortObjectInspector {

  protected OrcLazyShortObjectInspector() {
    super(PrimitiveObjectInspectorUtils.shortTypeEntry);
  }

  @Override
  public short get(Object o) {
    return ((ShortWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyShort((OrcLazyShort) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    ShortWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : Short.valueOf(writable.get());
  }

}
