package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcUnion;
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.RunLengthByteReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;

public class LazyUnionTreeReader extends LazyTreeReader {

  private final LazyTreeReader[] fields;
  private RunLengthByteReader tags;

  public LazyUnionTreeReader(int columnId, long rowIndexStride, LazyTreeReader[] fields) {
    super(columnId, rowIndexStride);
    this.fields = fields;
  }

  @Override
  public Object next(Object previous) throws IOException {
    OrcUnion result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new OrcUnion();
      } else {
        result = (OrcUnion) previous;
      }
      byte tag = nextTag();
      Object previousVal = result.getObject();
      result.set(tag, fields[tag].getInComplexType(tag == result.getTag() ?
          previousVal : null));
    }
    return result;
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    super.seek(rowIndexEntry, backwards);
    for (LazyTreeReader field : fields) {
      field.seek(rowIndexEntry, backwards);
    }
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    for (int i = 0; i < fields.length; i++) {
      fields[i].startStripe(streams, encodings, indexes, rowBaseInStripe);
    }
    tags = new RunLengthByteReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.DATA)));
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    tags.seek(index);
  }

  public byte nextTag() throws IOException {
    return (byte) tags.next();
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    long[] counts = new long[fields.length];
    for(int i=0; i < numNonNullValues; ++i) {
      counts[tags.next()] += 1;
    }
    for(int i=0; i < counts.length; ++i) {
      fields[i].skipRowsInComplexType(counts[i]);
    }
  }
}
