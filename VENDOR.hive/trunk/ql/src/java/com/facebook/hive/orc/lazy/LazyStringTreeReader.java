package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;

public class LazyStringTreeReader extends LazyTreeReader {

  private LazyTreeReader reader;

  public LazyStringTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    // For each stripe, checks the encoding and initializes the appropriate reader
    switch (encodings.get(columnId).getKind()) {
      case DIRECT:
        reader = new LazyStringDirectTreeReader(columnId, rowIndexStride);
        break;
      case DICTIONARY:
        reader = new LazyStringDictionaryTreeReader(columnId, rowIndexStride);
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
    if (currentRow >= 2470000) {
      System.out.println(currentRow);
    }
    return reader.get(currentRow, previous);
  }
}
