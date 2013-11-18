package com.facebook.hive.orc.lazy;

import java.util.List;

import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcStruct;
import com.facebook.hive.orc.OrcUnion;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

public final class OrcLazyObjectInspectorUtils {
  private static final OrcLazyFloatObjectInspector FLOAT_OBJECT_INSPECTOR = new OrcLazyFloatObjectInspector();
  private static final OrcLazyDoubleObjectInspector DOUBLE_OBJECT_INSPECTOR = new OrcLazyDoubleObjectInspector();
  private static final OrcLazyBooleanObjectInspector BOOLEAN_OBJECT_INSPECTOR = new OrcLazyBooleanObjectInspector();
  private static final OrcLazyByteObjectInspector BYTE_OBJECT_INSPECTOR = new OrcLazyByteObjectInspector();
  private static final OrcLazyShortObjectInspector SHORT_OBJECT_INSPECTOR = new OrcLazyShortObjectInspector();
  private static final OrcLazyIntObjectInspector INT_OBJECT_INSPECTOR = new OrcLazyIntObjectInspector();
  private static final OrcLazyLongObjectInspector LONG_OBJECT_INSPECTOR = new OrcLazyLongObjectInspector();
  private static final OrcLazyBinaryObjectInspector BINARY_OBJECT_INSPECTOR = new OrcLazyBinaryObjectInspector();
  private static final OrcLazyStringObjectInspector STRING_OBJECT_INSPECTOR = new OrcLazyStringObjectInspector();
  private static final OrcLazyTimestampObjectInspector TIMESTAMP_OBJECT_INSPECTOR = new OrcLazyTimestampObjectInspector();

  public static ObjectInspector createLazyObjectInspector(TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) info).getPrimitiveCategory()) {
          case FLOAT:
            return FLOAT_OBJECT_INSPECTOR;
          case DOUBLE:
            return DOUBLE_OBJECT_INSPECTOR;
          case BOOLEAN:
            return BOOLEAN_OBJECT_INSPECTOR;
          case BYTE:
            return BYTE_OBJECT_INSPECTOR;
          case SHORT:
            return SHORT_OBJECT_INSPECTOR;
          case INT:
            return INT_OBJECT_INSPECTOR;
          case LONG:
            return LONG_OBJECT_INSPECTOR;
          case BINARY:
            return BINARY_OBJECT_INSPECTOR;
          case STRING:
            return STRING_OBJECT_INSPECTOR;
          case TIMESTAMP:
            return TIMESTAMP_OBJECT_INSPECTOR;
          default:
            throw new IllegalArgumentException("Unknown primitive type " +
              ((PrimitiveTypeInfo) info).getPrimitiveCategory());
        }
      case STRUCT:
        return new OrcLazyStructObjectInspector((StructTypeInfo) info);
      case UNION:
        return new OrcLazyUnionObjectInspector((UnionTypeInfo) info);
      case MAP:
        return new OrcLazyMapObjectInspector((MapTypeInfo) info);
      case LIST:
        return new OrcLazyListObjectInspector((ListTypeInfo) info);
      default:
        throw new IllegalArgumentException("Unknown type " +
          info.getCategory());
    }
  }

  public static ObjectInspector createLazyObjectInspector(int columnId,
                                               List<OrcProto.Type> types){
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case FLOAT:
        return FLOAT_OBJECT_INSPECTOR;
      case DOUBLE:
        return DOUBLE_OBJECT_INSPECTOR;
      case BOOLEAN:
        return BOOLEAN_OBJECT_INSPECTOR;
      case BYTE:
        return BYTE_OBJECT_INSPECTOR;
      case SHORT:
        return SHORT_OBJECT_INSPECTOR;
      case INT:
        return INT_OBJECT_INSPECTOR;
      case LONG:
        return LONG_OBJECT_INSPECTOR;
      case BINARY:
        return BINARY_OBJECT_INSPECTOR;
      case STRING:
        return STRING_OBJECT_INSPECTOR;
      case TIMESTAMP:
        return TIMESTAMP_OBJECT_INSPECTOR;
      case STRUCT:
        return new OrcLazyStructObjectInspector(columnId, types);
      case UNION:
        return new OrcLazyUnionObjectInspector(columnId, types);
      case MAP:
        return new OrcLazyMapObjectInspector(columnId, types);
      case LIST:
        return new OrcLazyListObjectInspector(columnId, types);
      default:
        throw new UnsupportedOperationException("Unknown type " +
          type.getKind());
    }
  }

  public static ObjectInspector createWritableObjectInspector(TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveTypeInfo) info).getPrimitiveCategory()) {
          case FLOAT:
            return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
          case DOUBLE:
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
          case BOOLEAN:
            return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
          case BYTE:
            return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
          case SHORT:
            return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
          case INT:
            return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
          case LONG:
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
          case BINARY:
            return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
          case STRING:
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          case TIMESTAMP:
            return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
          default:
            throw new IllegalArgumentException("Unknown primitive type " +
              ((PrimitiveTypeInfo) info).getPrimitiveCategory());
        }
      case STRUCT:
        return new OrcStruct.OrcStructInspector((StructTypeInfo) info);
      case UNION:
        return new OrcUnion.OrcUnionObjectInspector((UnionTypeInfo) info);
      case MAP:
        return new OrcStruct.OrcMapObjectInspector((MapTypeInfo) info);
      case LIST:
        return new OrcStruct.OrcListObjectInspector((ListTypeInfo) info);
      default:
        throw new IllegalArgumentException("Unknown type " +
          info.getCategory());
    }
  }

  public static ObjectInspector createWritableObjectInspector(int columnId,
                                               List<OrcProto.Type> types){
    OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case FLOAT:
        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
      case DOUBLE:
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      case BOOLEAN:
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      case BYTE:
        return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
      case SHORT:
        return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
      case INT:
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      case LONG:
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      case BINARY:
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      case STRING:
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      case TIMESTAMP:
        return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
      case STRUCT:
        return new OrcStruct.OrcStructInspector(columnId, types);
      case UNION:
        return new OrcUnion.OrcUnionObjectInspector(columnId, types);
      case MAP:
        return new OrcStruct.OrcMapObjectInspector(columnId, types);
      case LIST:
        return new OrcStruct.OrcListObjectInspector(columnId, types);
      default:
        throw new UnsupportedOperationException("Unknown type " +
          type.getKind());
    }
  }
}
