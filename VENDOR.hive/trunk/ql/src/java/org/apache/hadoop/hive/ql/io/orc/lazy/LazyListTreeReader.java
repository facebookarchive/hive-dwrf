package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerReader;
import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;

public class LazyListTreeReader extends LazyTreeReader {
  private final LazyTreeReader elementReader;
  private RunLengthIntegerReader lengths;

  public LazyListTreeReader(int columnId, long rowIndexStride, LazyTreeReader elementReader) {
    super(columnId, rowIndexStride);
    this.elementReader = elementReader;
  }

  @Override
  public Object next(Object previous) throws IOException {
    List<Object> result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new ArrayList<Object>();
      } else {
        result = (ArrayList<Object>) previous;
      }
      int prevLength = result.size();
      int length = nextLength();
      // extend the list to the new length
      for(int i=prevLength; i < length; ++i) {
        result.add(null);
      }
      // read the new elements into the array
      for(int i=0; i< length; i++) {
        result.set(i, elementReader.getInComplexType(i < prevLength ?
            result.get(i) : null));
      }
      // remove any extra elements
      for(int i=prevLength - 1; i >= length; --i) {
        result.remove(i);
      }
    }
    return result;
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    super.seek(rowIndexEntry, backwards);
    elementReader.seek(rowIndexEntry, backwards);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams,
      List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes,
      long rowBaseInStripe
     ) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    elementReader.startStripe(streams, encodings, indexes, rowBaseInStripe);
    lengths = new RunLengthIntegerReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.LENGTH)), false, WriterImpl.INT_BYTE_SIZE);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    lengths.seek(index);
  }

  public int nextLength() throws IOException {
    return (int) lengths.next();
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    long childSkip = 0;
    for(long i=0; i < numNonNullValues; ++i) {
      childSkip += lengths.next();
    }
    elementReader.skipRowsInComplexType(childSkip);
  }
}
