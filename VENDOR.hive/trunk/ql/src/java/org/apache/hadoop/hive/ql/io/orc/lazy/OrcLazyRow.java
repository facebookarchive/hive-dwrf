package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.orc.InStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;

public class OrcLazyRow extends OrcLazyStruct {

  private OrcLazyObject[] fields;

  public OrcLazyRow(OrcLazyObject[] fields) {
    super(null);
    this.fields = fields;
  }

  @Override
  public void next() {
    super.next();
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.next();
      }
    }
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.startStripe(streams, encodings, indexes, rowBaseInStripe);
      }
    }
  }

  @Override
  public Object materialize(long row, Object previous) throws IOException {
    OrcStruct previousRow;
    if (previous != null) {
      previousRow = (OrcStruct) previous;
      previousRow.setNumFields(fields.length);
    } else {
      previousRow = new OrcStruct(fields.length);
    }
    for (int i = 0; i < fields.length; i++) {
      previousRow.setFieldValue(i, fields[i]);
    }
    return previousRow;
  }

  @Override
  public void seekToRow(long rowNumber) throws IOException {
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.seekToRow(rowNumber);
      }
    }
  }

  public int getNumFields() {
    return fields.length;
  }

  public OrcLazyObject getFieldValue(int index) {
    if (index >= fields.length) {
      return null;
    }

    return fields[index];
  }

  public void reset(OrcLazyRow other) throws IOException {
    this.fields = other.getRawFields();
    seekToRow(0);
  }

  protected OrcLazyObject[] getRawFields() {
    return fields;
  }
}
