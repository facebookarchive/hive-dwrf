package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.io.Writable;

public abstract class OrcLazyPrimitiveObjectInspector<T extends OrcLazyObject, U extends Writable> extends AbstractPrimitiveObjectInspector {

  protected OrcLazyPrimitiveObjectInspector(PrimitiveTypeEntry typeEntry) {
    super(typeEntry);
  }

  @Override
  public U getPrimitiveWritableObject(Object o) {
    try {
      return o == null ? null : (U)((T)o).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean preferWritable() {
    return true;
  }

}
