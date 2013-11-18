package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;

public class LazyShortTreeReader extends LazyIntTreeReader {

  public LazyShortTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    switch (encodings.get(columnId).getKind()) {
      case DICTIONARY:
        reader = new LazyShortDictionaryTreeReader(columnId, rowIndexStride);
        break;
      case DIRECT:
        reader = new LazyShortDirectTreeReader(columnId, rowIndexStride);
        break;
      default:
        throw new IllegalArgumentException("Unsupported encoding " +
            encodings.get(columnId).getKind());
    }
    reader.startStripe(streams, encodings, indexes, rowBaseInStripe);
  }

}
