package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class OrcLazyByteObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyByte, ByteWritable> implements
    ByteObjectInspector {

  protected OrcLazyByteObjectInspector() {
    super(PrimitiveObjectInspectorUtils.byteTypeEntry);
  }

  @Override
  public byte get(Object o) {
    return ((ByteWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyByte((OrcLazyByte) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    ByteWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : Byte.valueOf(writable.get());
  }
}
