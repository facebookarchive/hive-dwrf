package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.BitFieldReader;
import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.io.BooleanWritable;

public class LazyBooleanTreeReader extends LazyTreeReader {

  private BitFieldReader reader = null;

  public LazyBooleanTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    reader = new BitFieldReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.DATA)));
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    reader.seek(index);
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skip(numNonNullValues);
  }

  @Override
  public Object next(Object previous) throws IOException {
    BooleanWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new BooleanWritable();
      } else {
        result = (BooleanWritable) previous;
      }
      result.set(reader.next() == 1);
    }
    return result;
  }
}
