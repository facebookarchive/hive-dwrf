package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;

public class OrcLazyListObjectInspector implements ListObjectInspector {

  private final ObjectInspector child;

  public OrcLazyListObjectInspector(ListTypeInfo info) {
    child = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getListElementTypeInfo());
  }

  public OrcLazyListObjectInspector(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    child = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(0), types);
  }

  @Override
  public List<?> getList(Object data) {
    if (data == null) {
      return null;
    }

    try {
      return (List<?>) ((OrcLazyList) data).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }

    return getList(data).get(index);
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return child;
  }

  @Override
  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }

    return getList(data).size();
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  @Override
  public String getTypeName() {
    return "array<" + child.getTypeName() + ">";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      ObjectInspector other = ((OrcLazyListObjectInspector) o).child;
      return other.equals(child);
    }
  }
}
