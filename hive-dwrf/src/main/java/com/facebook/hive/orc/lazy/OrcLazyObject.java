//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.hive.orc.lazy;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;


public abstract class OrcLazyObject implements Writable {
  private long currentRow = 0;
  private final LazyTreeReader treeReader;
  protected Object previous;
  private boolean nextIsNull;
  private boolean materialized;
  private boolean writableCreated = false; //< Have we created a writable object for primitive types?
  private boolean nextIsNullSet = false;

  public OrcLazyObject(LazyTreeReader treeReader) {
    this.treeReader = treeReader;
  }

  public OrcLazyObject(OrcLazyObject copy) {
    materialized = copy.materialized;
    writableCreated = copy.writableCreated;
    currentRow = copy.currentRow;
    nextIsNull = copy.nextIsNull;
    nextIsNullSet = copy.nextIsNullSet;
    previous = copy.previous;
    treeReader = copy.treeReader;
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
      previous = materialize(currentRow, previous);
      materialized = true;
      writableCreated = true;
      nextIsNull = previous == null;
      nextIsNullSet = true;
    } else if (nextIsNull) {
      // If the value has been materialized and nextIsNull then the value is null
      return null;
    }
    if (!writableCreated) {
      // The client used a primitive materialization method, but is asking
      // for a writable now. Create it from the latest value.
      //
      previous = treeReader.createWritableFromLatest(previous);
      writableCreated = true;
    }
    // else 'previous' is already set to the required Writable object.
    return previous;
  }

  protected Object materialize(long row, Object previous) throws IOException {
    return treeReader.get(row, previous);
  }

  /**
   * An exception to indicate null values when materializing primitives.
   */
  protected final static class ValueNotPresentException extends IOException {
    public ValueNotPresentException() {}
    public ValueNotPresentException(String message) {super(message);}
  }

  // An interface for materializing primitive objects.
  protected interface Materializer {
    public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException;
  };

  // A double materializer
  protected final static Materializer doubleMaker =
    new Materializer() {
      public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException
        {
          treeReader.getDouble(currentRow);
        }
    };

  // A float materializer
  protected final static Materializer floatMaker =
    new Materializer() {
      public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException
        {
          treeReader.getFloat(currentRow);
        }
    };

  // A boolean materializer
  protected final static Materializer booleanMaker =
    new Materializer() {
      public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException
        {
          treeReader.getBoolean(currentRow);
        }
    };

  // A long materializer
  protected final static Materializer longMaker =
    new Materializer() {
      public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException
        {
          treeReader.getLong(currentRow);
        }
    };

  // An int materializer
  protected final static Materializer intMaker =
    new Materializer() {
      public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException
        {
          treeReader.getInt(currentRow);
        }
    };

  // A short materializer
  protected final static Materializer shortMaker =
    new Materializer() {
      public void materialize(LazyTreeReader treeReader, long currentRow) throws IOException
        {
          treeReader.getShort(currentRow);
        }
    };


  // A Helper to materialize primitive data types
  public void materializeHelper(Materializer maker) throws IOException {
    if (!materialized) {
      try {
        maker.materialize(treeReader, currentRow);
        materialized = true;
        writableCreated = false;
        nextIsNull = false;
        nextIsNullSet = true;
      }
      catch (ValueNotPresentException e) {
        // Is the value null?
        //
        materialized = true;
        writableCreated = true; // We have effectively created a writable
        nextIsNull = true;
        nextIsNullSet = true;
        throw new IOException("Cannot materialize primitive: value not present");
      }
    }
    else if (nextIsNull) {
      throw new IOException("Cannot materialize primitive: value not present.");
    }
  }

  public double materializeDouble() throws IOException {
    materializeHelper(doubleMaker);
    return treeReader.nextDouble(false);
  }

  public float materializeFloat() throws IOException {
    materializeHelper(floatMaker);
    return treeReader.nextFloat(false);
  }

  public boolean materializeBoolean() throws IOException {
    materializeHelper(booleanMaker);
    return treeReader.nextBoolean(false);
  }

  public long materializeLong() throws IOException {
    materializeHelper(longMaker);
    return treeReader.nextLong(false);
  }

  public int materializeInt() throws IOException {
    materializeHelper(intMaker);
    return treeReader.nextInt(false);
  }

  public short materializeShort() throws IOException {
    materializeHelper(shortMaker);
    return treeReader.nextShort(false);
  }


  public void next() {
    currentRow++;
    materialized = false;
    writableCreated = false;
    nextIsNullSet = false;
  }

  public boolean nextIsNull() throws IOException {
    if (!nextIsNullSet) {
      nextIsNull = treeReader.nextIsNull(currentRow);
      nextIsNullSet = true;
      // If the next value is null, we've essentially just materialized the value
      materialized = nextIsNull;
      writableCreated = nextIsNull;
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

  @Override
  public int hashCode() {
    try {
      materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return nextIsNull ? 0 : previous.hashCode();
  }

  public void close() throws IOException {
    treeReader.close();
  }
}
