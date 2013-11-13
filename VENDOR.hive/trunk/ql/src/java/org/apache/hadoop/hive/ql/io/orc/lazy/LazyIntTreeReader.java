package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.PositionProvider;
import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;

public class LazyIntTreeReader extends LazyTreeReader {

  protected LazyTreeReader reader;

  public LazyIntTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    switch (encodings.get(columnId).getKind()) {
      case DICTIONARY:
        reader = new LazyIntDictionaryTreeReader(columnId, rowIndexStride);
        break;
      case DIRECT:
        reader = new LazyIntDirectTreeReader(columnId, rowIndexStride);
        break;
      default:
        throw new IllegalArgumentException("Unsupported encoding " +
            encodings.get(columnId).getKind());
    }
    reader.startStripe(streams, encodings, indexes, rowBaseInStripe);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    reader.seek(index);
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    reader.seek(rowIndexEntry, backwards);
  }

  @Override
  public boolean nextIsNull(long currentRow) throws IOException {
    return reader.nextIsNull(currentRow);
  }

  @Override
  public boolean nextIsNullInComplexType() throws IOException {
    return reader.nextIsNullInComplexType();
  }

  @Override
  public Object getInComplexType(Object previous) throws IOException {
    return reader.getInComplexType(previous);
  }

  @Override
  public Object get(long currentRow, Object previous) throws IOException {
    return reader.get(currentRow, previous);
  }

  @Override
  public Object next(Object previous) throws IOException {
    return reader.next(previous);
  }

  @Override
  public void skipRowsInComplexType(long numRows) throws IOException {
    reader.skipRowsInComplexType(numRows);
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skipRows(numNonNullValues);
  }
}
