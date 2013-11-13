package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;

public class OrcLazyBinaryObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyBinary, BytesWritable> implements
    BinaryObjectInspector {

  protected OrcLazyBinaryObjectInspector() {
    super(PrimitiveObjectInspectorUtils.binaryTypeEntry);
  }

  @Override
  public byte[] getPrimitiveJavaObject(Object o) {
    BytesWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : LazyUtils.createByteArray(writable);
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyBinary((OrcLazyBinary) o);
  }
}
