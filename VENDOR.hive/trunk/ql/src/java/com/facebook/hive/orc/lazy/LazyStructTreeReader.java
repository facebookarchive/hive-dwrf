package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcStruct;
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;

public class LazyStructTreeReader extends LazyTreeReader {

  private final LazyTreeReader[] fields;
  private final List<String> fieldNames;

  public LazyStructTreeReader(int columnId, long rowIndexStride, LazyTreeReader[] fields,
      List<String> fieldNames) throws IOException {
    super(columnId, rowIndexStride);
    this.fields = fields;
    this.fieldNames = fieldNames;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    for(LazyTreeReader field: fields) {
      if (field != null) {
        field.skipRowsInComplexType(numNonNullValues);
      }
    }
  }

  @Override
  public Object next(Object previous) throws IOException {
    OrcStruct result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new OrcStruct(fieldNames);
      } else {
        result = (OrcStruct) previous;

        // If the input format was initialized with a file with a
        // different number of fields, the number of fields needs to
        // be updated to the correct number
        result.setFieldNames(fieldNames);
      }
      for(int i=0; i < fields.length; ++i) {
        if (fields[i] != null) {
          result.setFieldValue(i, fields[i].getInComplexType(result.getFieldValue(i)));
        }
      }
    }
    return result;
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams,
      List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes,
      long rowBaseInStripe
     ) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    for (int i = 0; i < fields.length; i++) {
      if (fields[i] != null) {
        fields[i].startStripe(streams, encodings, indexes, rowBaseInStripe);
      }
    }
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    super.seek(rowIndexEntry, backwards);
    for (LazyTreeReader field : fields) {
      if (field != null) {
        field.seek(rowIndexEntry, backwards);
      }
    }
  }

  @Override
  protected void seek(PositionProvider index) throws IOException {
    // Most tree readers have streams besides the present stream, e.g. the data for a simple type
    // or the length of a complex type.  The only data structs contain besides whether or not
    // they're null is the fields themselves, each of which has its own tree reader, so nothing
    // to do here.
  }
}
