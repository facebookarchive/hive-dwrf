package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.SerializationUtils;
import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.io.FloatWritable;

public class LazyFloatTreeReader extends LazyTreeReader {

  private InStream stream;

  public LazyFloatTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    StreamName name = new StreamName(columnId,
        OrcProto.Stream.Kind.DATA);
    stream = streams.get(name);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    stream.seek(index);
  }

  @Override
  public Object next(Object previous) throws IOException {
    FloatWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new FloatWritable();
      } else {
        result = (FloatWritable) previous;
      }
      result.set(SerializationUtils.readFloat(stream));
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    for(int i=0; i < numNonNullValues; ++i) {
      SerializationUtils.readFloat(stream);
    }
  }
}
