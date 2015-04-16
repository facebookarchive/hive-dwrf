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
package com.facebook.hive.orc;

import com.facebook.hive.orc.compression.CompressionCodec;
import com.facebook.hive.orc.lazy.LazyBinaryTreeReader;
import com.facebook.hive.orc.lazy.LazyBooleanTreeReader;
import com.facebook.hive.orc.lazy.LazyByteTreeReader;
import com.facebook.hive.orc.lazy.LazyDoubleTreeReader;
import com.facebook.hive.orc.lazy.LazyFloatTreeReader;
import com.facebook.hive.orc.lazy.LazyIntTreeReader;
import com.facebook.hive.orc.lazy.LazyListTreeReader;
import com.facebook.hive.orc.lazy.LazyLongTreeReader;
import com.facebook.hive.orc.lazy.LazyMapTreeReader;
import com.facebook.hive.orc.lazy.LazyShortTreeReader;
import com.facebook.hive.orc.lazy.LazyStringTreeReader;
import com.facebook.hive.orc.lazy.LazyStructTreeReader;
import com.facebook.hive.orc.lazy.LazyTimestampTreeReader;
import com.facebook.hive.orc.lazy.LazyTreeReader;
import com.facebook.hive.orc.lazy.LazyUnionTreeReader;
import com.facebook.hive.orc.lazy.OrcLazyBinary;
import com.facebook.hive.orc.lazy.OrcLazyBoolean;
import com.facebook.hive.orc.lazy.OrcLazyByte;
import com.facebook.hive.orc.lazy.OrcLazyDouble;
import com.facebook.hive.orc.lazy.OrcLazyFloat;
import com.facebook.hive.orc.lazy.OrcLazyInt;
import com.facebook.hive.orc.lazy.OrcLazyList;
import com.facebook.hive.orc.lazy.OrcLazyLong;
import com.facebook.hive.orc.lazy.OrcLazyMap;
import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.facebook.hive.orc.lazy.OrcLazyShort;
import com.facebook.hive.orc.lazy.OrcLazyString;
import com.facebook.hive.orc.lazy.OrcLazyStruct;
import com.facebook.hive.orc.lazy.OrcLazyTimestamp;
import com.facebook.hive.orc.lazy.OrcLazyUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RecordReaderImpl implements RecordReader {

  /** File being read by this reader */
  private final FSDataInputStream file;

  /** Row number of the first row being read by this reader */
  private final long firstRow;

  /** Information of all the stripes that can be read by this reader */
  private final List<StripeInformation> stripes = new ArrayList<>();
  private OrcProto.StripeFooter stripeFooter;

  /** Count of all the rows accessible from this reader */
  private final long totalRowCount;
  private final CompressionCodec codec;
  private final int bufferSize;

  /**
   * For each column, indicates if the reader should read the stream or not
   * If this is set to `null`, all columns are read. */
  private final boolean[] included;
  private final long rowIndexStride;
  private long rowInStripe = 0;
  private int currentStripe = 0;
  private long rowBaseInStripe = 0;
  private long rowCountInStripe = 0;
  private final Map<StreamName, InStream> streams = new HashMap<>();
  private OrcLazyRow reader;
  private final OrcProto.RowIndex[] indexes;
  private final int readStrides;
  private final boolean readEagerlyFromHdfs;
  private final long readEagerlyFromHdfsBytes;

  /**
   * Constructor
   *
   * @param stripes Stripe information for all the stripes in the file being read
   * @param fileSystem File system object
   * @param path Path to the file being read
   * @param offset start byte offset in the file for the reader
   * @param length a number of bytes to be read from the start offset
   * @param types information about all the columns
   * @param codec compression codec being used
   * @param bufferSize
   * @param included columns to be included by the reader
   * @param strideRate
   * @param conf configuration object
   * @throws IOException
   */
  RecordReaderImpl(Iterable<StripeInformation> stripes,
                   FileSystem fileSystem,
                   Path path,
                   long offset,
                   long length,
                   List<OrcProto.Type> types,
                   CompressionCodec codec,
                   int bufferSize,
                   boolean[] included,
                   long strideRate,
                   Configuration conf) throws IOException {
    this.file = fileSystem.open(path);
    this.codec = codec;
    this.bufferSize = bufferSize;
    this.included = included;
    this.readStrides = OrcConf.getIntVar(conf, OrcConf.ConfVars.HIVE_ORC_READ_COMPRESSION_STRIDES);
    this.readEagerlyFromHdfs = OrcConf.getBoolVar(conf, OrcConf.ConfVars.HIVE_ORC_EAGER_HDFS_READ);
    this.readEagerlyFromHdfsBytes =
      OrcConf.getLongVar(conf, OrcConf.ConfVars.HIVE_ORC_EAGER_HDFS_READ_BYTES);

    // For all stripes with start offset before the {@param offset}, data will
    // not be read from these stripes. skippedRows is sum of number of rows in
    // these stripes. This also indicates the row number of the first row
    // pointed by this reader
    long skippedRows = 0;

    // Number of rows which can be read using this reader
    long rows = 0;

    for (final StripeInformation stripe : stripes) {
      long stripeStart = stripe.getOffset();
      if (offset > stripeStart) {
        skippedRows += stripe.getNumberOfRows();
      } else if (stripeStart < offset + length) {
        this.stripes.add(stripe);
        rows += stripe.getNumberOfRows();
      }
    }
    firstRow = skippedRows;
    totalRowCount = rows;
    indexes = new OrcProto.RowIndex[types.size()];
    rowIndexStride = strideRate;
    reader = createLazyRow(types, included);
    if (this.stripes.size() > 0) {
      readStripe();
    }
  }

  OrcLazyRow createLazyRow(List<OrcProto.Type> types, boolean[] included) throws IOException {
    OrcProto.Type type = types.get(0);
    int structFieldCount = type.getFieldNamesCount();
    OrcLazyObject[] structFields = new OrcLazyObject[structFieldCount];
    for (int i = 0; i < structFieldCount; i++) {
      int subtype = type.getSubtypes(i);
      if (included == null || included[subtype]) {
        structFields[i] = createLazyObject(subtype, types, included);
      }
    }
    return new OrcLazyRow(structFields, type.getFieldNamesList());
  }

  LazyTreeReader createLazyTreeReader(int columnId,
                                      List<OrcProto.Type> types,
                                      boolean[] included) throws IOException {
    final OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
        return new LazyBooleanTreeReader(columnId, rowIndexStride);
      case BYTE:
        return new LazyByteTreeReader(columnId, rowIndexStride);
      case DOUBLE:
        return new LazyDoubleTreeReader(columnId, rowIndexStride);
      case FLOAT:
        return new LazyFloatTreeReader(columnId, rowIndexStride);
      case SHORT:
        return new LazyShortTreeReader(columnId, rowIndexStride);
      case LONG:
        return new LazyLongTreeReader(columnId, rowIndexStride);
      case INT:
        return new LazyIntTreeReader(columnId, rowIndexStride);
      case STRING:
        return new LazyStringTreeReader(columnId, rowIndexStride);
      case BINARY:
        return new LazyBinaryTreeReader(columnId, rowIndexStride);
      case TIMESTAMP:
        return new LazyTimestampTreeReader(columnId, rowIndexStride);
      case STRUCT:
        final int structFieldCount = type.getFieldNamesCount();
        final LazyTreeReader[] structFields = new LazyTreeReader[structFieldCount];
        for (int i = 0; i < structFieldCount; i++) {
          int subtype = type.getSubtypes(i);
          if (included == null || included[subtype]) {
            structFields[i] = createLazyTreeReader(subtype, types, included);
          }
        }
        return new LazyStructTreeReader(columnId, rowIndexStride, structFields, type.getFieldNamesList());

      case LIST:
        final LazyTreeReader elementReader = createLazyTreeReader(type.getSubtypes(0), types, included);
        return new LazyListTreeReader(columnId, rowIndexStride, elementReader);

      case MAP:
        final LazyTreeReader keyReader = createLazyTreeReader(type.getSubtypes(0), types, included);
        final LazyTreeReader valueReader = createLazyTreeReader(type.getSubtypes(1), types, included);
        return new LazyMapTreeReader(columnId, rowIndexStride, keyReader, valueReader);

      case UNION:
        final int unionFieldCount = type.getSubtypesCount();
        final LazyTreeReader[] unionFields = new LazyTreeReader[unionFieldCount];
        for(int i = 0; i < unionFieldCount; ++i) {
          unionFields[i] = createLazyTreeReader(type.getSubtypes(i), types, included);
        }
        return new LazyUnionTreeReader(columnId, rowIndexStride, unionFields);

      default:
        throw new IllegalArgumentException("Unsupported type " + type.getKind());
    }
  }

  OrcLazyObject createLazyObject(int columnId,
                                 List<OrcProto.Type> types,
                                 boolean[] included) throws IOException {
    final OrcProto.Type type = types.get(columnId);
    switch (type.getKind()) {
      case BOOLEAN:
        return new OrcLazyBoolean((LazyBooleanTreeReader)createLazyTreeReader(columnId, types, included));
      case BYTE:
        return new OrcLazyByte((LazyByteTreeReader)createLazyTreeReader(columnId, types, included));
      case DOUBLE:
        return new OrcLazyDouble((LazyDoubleTreeReader)createLazyTreeReader(columnId, types, included));
      case FLOAT:
        return new OrcLazyFloat((LazyFloatTreeReader)createLazyTreeReader(columnId, types, included));
      case SHORT:
        return new OrcLazyShort((LazyShortTreeReader)createLazyTreeReader(columnId, types, included));
      case LONG:
        return new OrcLazyLong((LazyLongTreeReader)createLazyTreeReader(columnId, types, included));
      case INT:
        return new OrcLazyInt((LazyIntTreeReader)createLazyTreeReader(columnId, types, included));
      case STRING:
        return new OrcLazyString((LazyStringTreeReader)createLazyTreeReader(columnId, types, included));
      case BINARY:
        return new OrcLazyBinary((LazyBinaryTreeReader)createLazyTreeReader(columnId, types, included));
      case TIMESTAMP:
        return new OrcLazyTimestamp((LazyTimestampTreeReader)createLazyTreeReader(columnId, types, included));
      case STRUCT:
        return new OrcLazyStruct((LazyStructTreeReader)createLazyTreeReader(columnId, types, included));
      case LIST:
        return new OrcLazyList((LazyListTreeReader)createLazyTreeReader(columnId, types, included));
      case MAP:
        return new OrcLazyMap((LazyMapTreeReader)createLazyTreeReader(columnId, types, included));
      case UNION:
        return new OrcLazyUnion((LazyUnionTreeReader)createLazyTreeReader(columnId, types, included));
      default:
        throw new IllegalArgumentException("Unsupported type " + type.getKind());
    }
  }

  OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
    long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
    int tailLength = (int) stripe.getFooterLength();

    return OrcProto.StripeFooter.parseFrom(InStream.create("stripe-footer", file, offset,
        tailLength, codec, bufferSize));
  }

  private void readEntireStripeEagerly(StripeInformation stripe, long offset) throws IOException {
    final byte[] buffer = new byte[(int) (stripe.getDataLength())];
    file.seek(offset + stripe.getIndexLength());
    file.readFully(buffer, 0, buffer.length);

    int sectionOffset = 0;
    for(OrcProto.Stream section: stripeFooter.getStreamsList()) {
      final StreamName.Area area = StreamName.getArea(section.getKind());

      if (area == StreamName.Area.DATA || area == StreamName.Area.DICTIONARY) {
        final int sectionLength = (int) section.getLength();
        final StreamName name = new StreamName(section.getColumn(), section.getKind());
        final ByteBuffer sectionBuffer = ByteBuffer.wrap(buffer, sectionOffset, sectionLength);
        streams.put(name, InStream.create(name.toString(), sectionBuffer, codec, bufferSize,
            section.getUseVInts()));
        sectionOffset += sectionLength;
      }
    }
  }

  private void readEntireStripeLazily(StripeInformation stripe, long offset) throws IOException {
    int sectionOffset = 0;
    for(OrcProto.Stream section: stripeFooter.getStreamsList()) {
      if (StreamName.getArea(section.getKind()) == StreamName.Area.DATA ||
          StreamName.getArea(section.getKind()) == StreamName.Area.DICTIONARY) {
        final int sectionLength = (int) section.getLength();
        final StreamName name = new StreamName(section.getColumn(), section.getKind());
        streams.put(name, InStream.create(name.toString(), file,
            offset + stripe.getIndexLength() + sectionOffset, sectionLength, codec, bufferSize,
            section.getUseVInts(), readStrides));
        sectionOffset += sectionLength;
      }
    }
  }

  private void readIncludedStreamsEagerly(StripeInformation stripe,
      List<OrcProto.Stream> streamList, long offset, int currentSection) throws IOException {
    long sectionOffset = stripe.getIndexLength();
    while (currentSection < streamList.size()) {
      int bytes = 0;

      // find the first section that shouldn't be read
      int excluded = currentSection;
      while (excluded < streamList.size() && included[streamList.get(excluded).getColumn()]) {
        bytes += streamList.get(excluded++).getLength();
      }

      // actually read the bytes as a big chunk
      if (currentSection < excluded) {
        byte[] buffer = new byte[bytes];
        file.seek(offset + sectionOffset);
        file.readFully(buffer, 0, bytes);
        sectionOffset += bytes;

        // create the streams for the sections we just read
        bytes = 0;
        while (currentSection < excluded) {
          OrcProto.Stream section = streamList.get(currentSection);
          StreamName name =
            new StreamName(section.getColumn(), section.getKind());
          this.streams.put(name,
              InStream.create(name.toString(), ByteBuffer.wrap(buffer, bytes,
                  (int) section.getLength()), codec, bufferSize, section.getUseVInts()));
          currentSection++;
          bytes += section.getLength();
        }
      }

      // skip forward until we get back to a section that we need
      while (currentSection < streamList.size() && !included[streamList.get(currentSection).getColumn()]) {
        sectionOffset += streamList.get(currentSection).getLength();
        currentSection++;
      }
    }
  }

  private void readIncludedStreamsLazily(StripeInformation stripe,
      List<OrcProto.Stream> streamList, long offset, int currentSection) throws IOException {
    long sectionOffset = stripe.getIndexLength();
    while (currentSection < streamList.size()) {
      if (included[streamList.get(currentSection).getColumn()]) {
        OrcProto.Stream section = streamList.get(currentSection);
        StreamName name =
          new StreamName(section.getColumn(), section.getKind());
        this.streams.put(name,
            InStream.create(name.toString(), file, offset + sectionOffset,
                (int) section.getLength(), codec, bufferSize, section.getUseVInts(),
                readStrides));
      }
      sectionOffset += streamList.get(currentSection).getLength();
      currentSection += 1;
    }
  }

  /**
   * Calculates the total number of bytes to be read for the data streams.
   * If this sum is <= {@code readEagerlyFromHdfsBytes}, return true else return false
   */
  protected boolean shouldReadEagerly(StripeInformation stripe, int currentSection) {
    if (readEagerlyFromHdfsBytes <= 0) {
      return readEagerlyFromHdfs;
    }

    long inputBytes = 0;
    if (included == null) {
      // This means there is no projection and all the columns would be read
      inputBytes = stripe.getDataLength();
    } else {
      // Get a sum of lengths of all the streams desired to be read
      List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
      for (int i = currentSection; i < streamList.size(); i++) {
        if (included[streamList.get(i).getColumn()]) {
          inputBytes += streamList.get(i).getLength();
        }
      }
    }

    return inputBytes <= readEagerlyFromHdfsBytes;
  }

  private void readStripe() throws IOException {
    final StripeInformation stripe = stripes.get(currentStripe);
    stripeFooter = readStripeFooter(stripe);
    final long offset = stripe.getOffset();
    streams.clear();

    // if we aren't projecting columns, just read the whole stripe
    if (included == null) {
      if (shouldReadEagerly(stripe, 0)) {
        readEntireStripeEagerly(stripe, offset);
      } else {
        readEntireStripeLazily(stripe, offset);
      }
    } else {
      List<OrcProto.Stream> streamList = stripeFooter.getStreamsList();
      // the index of the current section
      int currentSection = 0;
      while (currentSection < streamList.size() &&
          StreamName.getArea(streamList.get(currentSection).getKind()) != StreamName.Area.DATA &&
          StreamName.getArea(streamList.get(currentSection).getKind()) !=
            StreamName.Area.DICTIONARY) {
        currentSection += 1;
      }

      if (shouldReadEagerly(stripe, currentSection)) {
        readIncludedStreamsEagerly(stripe, streamList, offset, currentSection);
      } else {
        readIncludedStreamsLazily(stripe, streamList, offset, currentSection);
      }
    }

    rowInStripe = 0;
    rowCountInStripe = stripe.getNumberOfRows();
    rowBaseInStripe = 0;
    for(int i=0; i < currentStripe; ++i) {
      rowBaseInStripe += stripes.get(i).getNumberOfRows();
    }
    readRowIndex();
    ReaderWriterProfiler.start(ReaderWriterProfiler.Counter.DESERIALIZATION_TIME);
    reader.startStripe(streams, stripeFooter.getColumnsList(), indexes, rowBaseInStripe);
    ReaderWriterProfiler.end(ReaderWriterProfiler.Counter.DESERIALIZATION_TIME);

    // We don't need the indices anymore, so free them
    for(int i=0; i < indexes.length; ++i) {
      indexes[i] = null;
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    return rowInStripe < rowCountInStripe || currentStripe < stripes.size() - 1;
  }

  @Override
  public Object next(Object previous) throws IOException {
    if (rowInStripe >= rowCountInStripe) {
      reader.close();
      currentStripe += 1;
      readStripe();
    }
    rowInStripe += 1;

    if (previous == null) {
      previous = reader;
    } else if (previous != reader) {
      ((OrcLazyRow) previous).reset(reader);
      reader = (OrcLazyRow) previous;
    }

    ((OrcLazyObject) previous).next();
    return previous;
  }

  @Override
  public void close() throws IOException {
    file.close();
    reader.close();
  }

  @Override
  public long getRowNumber() {
    return rowInStripe + rowBaseInStripe + firstRow;
  }

  /**
   * Return the fraction of rows that have been read from the selected section of the file
   * @return fraction between 0.0 and 1.0 of rows consumed
   */
  @Override
  public float getProgress() {
    return ((float) rowBaseInStripe + rowInStripe) / totalRowCount;
  }

  private int findStripe(long rowNumber) {
    if (rowNumber < 0) {
      throw new IllegalArgumentException("Seek to a negative row number " + rowNumber);
    } else if (rowNumber < firstRow) {
      throw new IllegalArgumentException("Seek before reader range. rowNumber = " + rowNumber +
                                             " firstRow = " + firstRow);
    }

    rowNumber -= firstRow;
    for (int i = 0; i < stripes.size(); i++) {
      final StripeInformation stripe = stripes.get(i);
      if (stripe.getNumberOfRows() > rowNumber) {
        return i;
      }
      rowNumber -= stripe.getNumberOfRows();
    }
    throw new IllegalArgumentException("Seek after the end of reader range. rowNumber = " +
                                           rowNumber);
  }

  private void readRowIndex() throws IOException {
    long offset = stripes.get(currentStripe).getOffset();
    for(OrcProto.Stream stream: stripeFooter.getStreamsList()) {
      if (stream.getKind() == OrcProto.Stream.Kind.ROW_INDEX) {
        int col = stream.getColumn();
        if ((included == null || included[col]) && indexes[col] == null) {
          indexes[col] = OrcProto.RowIndex.parseFrom(InStream.create("index",
              file, offset, (int) stream.getLength(), codec, bufferSize,
              stream.getUseVInts(), readStrides));
        }
      }
      offset += stream.getLength();
    }
  }

  @Override
  public void seekToRow(long rowNumber) throws IOException {
    // Update the stripe
    int rightStripe = findStripe(rowNumber);
    if (rightStripe != this.currentStripe) {
      this.currentStripe = rightStripe;
      readStripe();
    }

    // Update the row number within the stripe
    this.rowInStripe = rowNumber - this.rowBaseInStripe - this.firstRow;

    // Update the reader
    this.reader.seekToRow(rowNumber - this.firstRow);
  }

  @Override
  public OrcLazyRow getReader() {
    return reader;
  }
}
