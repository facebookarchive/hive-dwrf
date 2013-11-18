package com.facebook.hive.orc.lazy;

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

public class OrcLazyTimestampObjectInspector extends
    OrcLazyPrimitiveObjectInspector<OrcLazyTimestamp, TimestampWritable> implements TimestampObjectInspector {

  protected OrcLazyTimestampObjectInspector() {
    super(PrimitiveObjectInspectorUtils.timestampTypeEntry);
  }

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    TimestampWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : writable.getTimestamp();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyTimestamp((OrcLazyTimestamp) o);
  }

}
