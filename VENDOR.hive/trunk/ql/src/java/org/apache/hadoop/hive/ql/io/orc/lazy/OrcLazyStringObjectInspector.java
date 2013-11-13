package org.apache.hadoop.hive.ql.io.orc.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class OrcLazyStringObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyString, Text>
    implements StringObjectInspector {

  protected OrcLazyStringObjectInspector() {
    super(PrimitiveObjectInspectorUtils.stringTypeEntry);
  }

  @Override
  public String getPrimitiveJavaObject(Object o) {
    Text text = getPrimitiveWritableObject(o);
    return text == null ? null : text.toString();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyString((OrcLazyString) o);
  }

}
