package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct.Field;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class OrcLazyRowObjectInspector extends OrcLazyStructObjectInspector {

  public OrcLazyRowObjectInspector(StructTypeInfo info) {
    ArrayList<String> fieldNames = info.getAllStructFieldNames();
    ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
    for(int i=0; i < fieldNames.size(); ++i) {
      fields.add(new Field(fieldNames.get(i),
          OrcLazyObjectInspectorUtils.createLazyObjectInspector(fieldTypes.get(i)), i));
    }
  }

  public OrcLazyRowObjectInspector(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    int fieldCount = type.getSubtypesCount();
    for(int i=0; i < fieldCount; ++i) {
      int fieldType = type.getSubtypes(i);
      fields.add(new Field(type.getFieldNames(i),
          OrcLazyObjectInspectorUtils.createLazyObjectInspector(fieldType, types), i));
    }
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    int offset = ((Field) fieldRef).getOffset();

    try {
      OrcLazyObject obj = ((OrcLazyRow) data).getFieldValue(offset);
      return obj == null || obj.nextIsNull() ? null : obj;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    for(StructField field: fields) {
      if (field.getFieldName().equals(fieldName)) {
        return field;
      }
    }
    return null;
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    OrcLazyRow row = (OrcLazyRow) data;
    int numFields = row.getNumFields();
    List<Object> result = new ArrayList<Object>(numFields);
    for (int i = 0; i < numFields; i++) {
      try {
        OrcLazyObject obj = (OrcLazyObject) row.getFieldValue(i);
        result.add(obj == null || obj.nextIsNull() ? null : obj);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }
    return result;
  }
}
