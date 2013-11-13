package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerReader;
import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;

public class LazyMapTreeReader extends LazyTreeReader {
  private final LazyTreeReader keyReader;
  private final LazyTreeReader valueReader;
  private RunLengthIntegerReader lengths;

  public LazyMapTreeReader(int columnId, long rowIndexStride, LazyTreeReader keyReader, LazyTreeReader valueReader) {
    super(columnId, rowIndexStride);
    this.keyReader = keyReader;
    this.valueReader = valueReader;
  }

  @Override
  public Object next(Object previous) throws IOException {
    Map<Object, Object> result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new HashMap<Object, Object>();
      } else {
        result = (HashMap<Object, Object>) previous;
      }
      // for now just clear and create new objects
      result.clear();
      int length = nextLength();
      // read the new elements into the array
      for(int i=0; i< length; i++) {
        result.put(keyReader.getInComplexType(null), valueReader.getInComplexType(null));
      }
    }
    return result;
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    super.seek(rowIndexEntry, backwards);
    keyReader.seek(rowIndexEntry, backwards);
    valueReader.seek(rowIndexEntry,backwards);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams,
      List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes,
      long rowBaseInStripe
     ) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    keyReader.startStripe(streams, encodings, indexes, rowBaseInStripe);
    valueReader.startStripe(streams, encodings, indexes, rowBaseInStripe);
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
    keyReader.skipRowsInComplexType(childSkip);
    valueReader.skipRowsInComplexType(childSkip);
  }
}
