package com.facebook.hive.orc.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.io.Writable;


public abstract class OrcLazyObject implements Writable {
  private long currentRow = 0;
  private LazyTreeReader treeReader;
  protected Object previous;
  private boolean nextIsNull;
  private boolean materialized;
  private boolean nextIsNullSet;

  public OrcLazyObject(LazyTreeReader treeReader) {
    this.treeReader = treeReader;
  }

  public OrcLazyObject(OrcLazyObject copy) {
    materialized = true;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
  }

  public LazyTreeReader getLazyTreeReader() {
    return treeReader;
  }

  public Object materialize() throws IOException {
    if (!materialized) {
      ReaderWriterProfiler.start(ReaderWriterProfiler.Counter.DECODING_TIME);
      previous = materialize(currentRow, previous);
      materialized = true;
      nextIsNull = previous == null;
      nextIsNullSet = true;
      ReaderWriterProfiler.end(ReaderWriterProfiler.Counter.DECODING_TIME);
    } else if (nextIsNull) {
      // If the value has been materialized and nextIsNull then the value is null
      return null;
    }
    return previous;
  }

  protected Object materialize(long row, Object previous) throws IOException {
    return treeReader.get(row, previous);
  }

  public void next() {
    currentRow++;
    materialized = false;
    nextIsNullSet = false;
  }

  public boolean nextIsNull() throws IOException {
    if (!nextIsNullSet) {
      ReaderWriterProfiler.start(ReaderWriterProfiler.Counter.DECODING_TIME);
      nextIsNull = treeReader.nextIsNull(currentRow);
      nextIsNullSet = true;
      // If the next value is null, we've essentially just materialized the value
      materialized = nextIsNull;
      ReaderWriterProfiler.end(ReaderWriterProfiler.Counter.DECODING_TIME);
    }
    return nextIsNull;
  }

  public void startStripe(Map<StreamName, InStream> streams,
      List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes,
      long rowBaseInStripe
     ) throws IOException {
    treeReader.startStripe(streams, encodings, indexes, rowBaseInStripe);
  }

  public void seekToRow(long rowNumber) throws IOException {
    currentRow = rowNumber;
  }
}
