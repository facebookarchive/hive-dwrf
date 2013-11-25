package com.facebook.hive.orc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class OrcTestUtils {
  public static class InnerStruct {
    Integer int1;
    Text string1 = new Text();
    InnerStruct(Integer int1, String string1) {
      this.int1 = int1;
      if (string1 == null) {
        this.string1 = null;
      } else {
        this.string1.set(string1);
      }
    }
  }

  public static class MiddleStruct {
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    public MiddleStruct(InnerStruct... items) {
      list.clear();
      for(InnerStruct item: items) {
        list.add(item);
      }
    }
  }

  /**
   *
   * BigRowTwoStrings.
   *
   * A BigRow with an additional String column,
   * useful for testing both dictionary and directly encoded strings
   *
   * Unfortunately this cannot simply extend BigRow in order to make the reflection
   * ObjectInspector work.
   */
  public static class BigRowTwoStrings {
    Boolean boolean1;
    Byte byte1;
    Short short1;
    Integer int1;
    Long long1;
    Float float1;
    Double double1;
    BytesWritable bytes1;
    Text string1;
    Text string2;
    MiddleStruct middle;
    List<InnerStruct> list = new ArrayList<InnerStruct>();
    Map<Text, InnerStruct> map = new HashMap<Text, InnerStruct>();

    public BigRowTwoStrings(Boolean b1, Byte b2, Short s1, Integer i1, Long l1, Float f1,
        Double d1, BytesWritable b3, String s2, String s3, MiddleStruct m1, List<InnerStruct> l2,
        Map<Text, InnerStruct> m2) {
      this.boolean1 = b1;
      this.byte1 = b2;
      this.short1 = s1;
      this.int1 = i1;
      this.long1 = l1;
      this.float1 = f1;
      this.double1 = d1;
      this.bytes1 = b3;
      if (s2 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s2);
      }
      if (s3 == null) {
        this.string2 = null;
      } else {
        this.string2 = new Text(s3);
      }
      this.middle = m1;
      this.list = l2;
      this.map = m2;
    }
  }

  public static class BigRow {
    Boolean boolean1;
    Byte byte1;
    Short short1;
    Integer int1;
    Long long1;
    Float float1;
    Double double1;
    BytesWritable bytes1;
    Text string1;
    MiddleStruct middle;
    List<InnerStruct> list = new ArrayList<InnerStruct>();
    Map<Text, InnerStruct> map = new HashMap<Text, InnerStruct>();

    public BigRow(Boolean b1, Byte b2, Short s1, Integer i1, Long l1, Float f1,
           Double d1,
           BytesWritable b3, String s2, MiddleStruct m1,
           List<InnerStruct> l2, Map<Text, InnerStruct> m2) {
      this.boolean1 = b1;
      this.byte1 = b2;
      this.short1 = s1;
      this.int1 = i1;
      this.long1 = l1;
      this.float1 = f1;
      this.double1 = d1;
      this.bytes1 = b3;
      if (s2 == null) {
        this.string1 = null;
      } else {
        this.string1 = new Text(s2);
      }
      this.middle = m1;
      this.list = l2;
      this.map = m2;
    }
  }

  public static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  public static Map<Text, InnerStruct> map(InnerStruct... items)  {
    Map<Text, InnerStruct> result = new HashMap<Text, InnerStruct>();
    for(InnerStruct i: items) {
      result.put(i == null || i.string1 == null ? null : new Text(i.string1), i);
    }
    return result;
  }

  public static List<InnerStruct> list(InnerStruct... items) {
    List<InnerStruct> result = new ArrayList<InnerStruct>();
    for(InnerStruct s: items) {
      result.add(s);
    }
    return result;
  }

  public static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for(int i=0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  public static ByteBuffer byteBuf(int... items) {
     ByteBuffer result = ByteBuffer.allocate(items.length);
    for(int item: items) {
      result.put((byte) item);
    }
    return result;
  }
}
