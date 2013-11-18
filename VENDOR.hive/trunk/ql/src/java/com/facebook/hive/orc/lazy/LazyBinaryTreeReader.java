package com.facebook.hive.orc.lazy;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;
import com.facebook.hive.orc.OrcProto.RowIndex;
import org.apache.hadoop.io.BytesWritable;

public class LazyBinaryTreeReader extends LazyTreeReader {

  private InStream stream;
  private RunLengthIntegerReader lengths;

  public LazyBinaryTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    StreamName name = new StreamName(columnId,
        OrcProto.Stream.Kind.DATA);
    stream = streams.get(name);
    lengths = new RunLengthIntegerReader(streams.get(new
        StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
        false, WriterImpl.INT_BYTE_SIZE);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    stream.seek(index);
    lengths.seek(index);
  }

  @Override
  public Object next(Object previous) throws IOException {
    BytesWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new BytesWritable();
      } else {
        result = (BytesWritable) previous;
      }
      int len = (int) lengths.next();
      result.setSize(len);
      int offset = 0;
      while (len > 0) {
        int written = stream.read(result.getBytes(), offset, len);
        if (written < 0) {
          throw new EOFException("Can't finish byte read from " + stream);
        }
        len -= written;
        offset += written;
      }
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    long lengthToSkip = 0;
    for(int i=0; i < numNonNullValues; ++i) {
      lengthToSkip += lengths.next();
    }
    stream.skip(lengthToSkip);
  }

}
