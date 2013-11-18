package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.OrcProto;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;

public class OrcLazyMapObjectInspector implements MapObjectInspector {

  private final ObjectInspector key;
  private final ObjectInspector value;

  public OrcLazyMapObjectInspector(MapTypeInfo info) {
    key = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getMapKeyTypeInfo());
    value = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getMapValueTypeInfo());
  }

  public OrcLazyMapObjectInspector(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    key = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(0), types);
    value = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(1), types);
  }

  @Override
  public Map<?, ?> getMap(Object data) {
    if (data == null) {
      return null;
    }
    try {
      return (Map<?, ?>) ((OrcLazyMap) data).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectInspector getMapKeyObjectInspector() {
    return key;
  }

  @Override
  public int getMapSize(Object data) {
    return getMap(data).size();
  }

  @Override
  public Object getMapValueElement(Object data, Object key) {
    return getMap(data).get(key);
  }

  @Override
  public ObjectInspector getMapValueObjectInspector() {
    return value;
  }

  @Override
  public Category getCategory() {
    return Category.MAP;
  }

  @Override
  public String getTypeName() {
    return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      OrcLazyMapObjectInspector other = (OrcLazyMapObjectInspector) o;
      return other.key.equals(key) && other.value.equals(value);
    }
  }

}
