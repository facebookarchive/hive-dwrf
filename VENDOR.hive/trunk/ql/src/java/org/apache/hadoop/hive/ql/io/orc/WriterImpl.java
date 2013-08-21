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

package org.apache.hadoop.hive.ql.io.orc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.RawDatasizeConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

/**
 * An ORC file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 *
 * This class is synchronized so that multi-threaded access is ok. In
 * particular, because the MemoryManager is shared between writers, this class
 * assumes that checkMemory may be called from a separate thread.
 */
class WriterImpl implements Writer, MemoryManager.Callback {

  private static final Log LOG = LogFactory.getLog(WriterImpl.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;
  private static final int MIN_ROW_INDEX_STRIDE = 1000;

  static final int SHORT_BYTE_SIZE = 2;
  static final int INT_BYTE_SIZE = 4;
  static final int LONG_BYTE_SIZE = 8;

  private final FileSystem fs;
  private final Path path;
  private final long stripeSize;
  private final int rowIndexStride;
  private final CompressionKind compress;
  private final CompressionCodec codec;
  private final int bufferSize;
  // the streams that make up the current stripe
  private final Map<StreamName, BufferedStream> streams =
    new TreeMap<StreamName, BufferedStream>();

  private FSDataOutputStream rawWriter = null;
  // the compressed metadata information outStream
  private OutStream writer = null;
  // a protobuf outStream around streamFactory
  private CodedOutputStream protobufWriter = null;
  private long headerLength;
  private int columnCount;
  private long rowCount = 0;
  private long rowsInStripe = 0;
  private int rowsInIndex = 0;
  private long rawDataSize = 0;
  private final List<OrcProto.StripeInformation> stripes =
    new ArrayList<OrcProto.StripeInformation>();
  private final Map<String, ByteString> userMetadata =
    new TreeMap<String, ByteString>();
  private final StreamFactory streamFactory = new StreamFactory();
  private final TreeWriter treeWriter;
  private final OrcProto.RowIndex.Builder rowIndex =
      OrcProto.RowIndex.newBuilder();
  private final boolean buildIndex;
  private final MemoryManager memoryManager;
  private final boolean useVInts;
  private final int dfsBytesPerChecksum;

  private final Configuration conf;

  WriterImpl(FileSystem fs,
             Path path,
             Configuration conf,
             ObjectInspector inspector,
             long stripeSize,
             CompressionKind compress,
             int bufferSize,
             int rowIndexStride,
             MemoryManager memoryManager) throws IOException {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
    this.stripeSize = stripeSize;
    this.compress = compress;
    this.bufferSize = bufferSize;
    this.rowIndexStride = rowIndexStride;
    this.memoryManager = memoryManager;
    buildIndex = rowIndexStride > 0;
    codec = createCodec(compress);
    useVInts = conf.getBoolean(HiveConf.ConfVars.HIVE_ORC_USE_VINTS.varname,
        HiveConf.ConfVars.HIVE_ORC_USE_VINTS.defaultBoolVal);
    treeWriter = createTreeWriter(inspector, streamFactory, false, conf, useVInts);
    dfsBytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    if (buildIndex && rowIndexStride < MIN_ROW_INDEX_STRIDE) {
      throw new IllegalArgumentException("Row stride must be at least " +
          MIN_ROW_INDEX_STRIDE);
    }
    // ensure that we are able to handle callbacks before we register ourselves
    memoryManager.addWriter(path, stripeSize, this);
  }

  static CompressionCodec createCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        try {
          Class<? extends CompressionCodec> lzo =
              (Class<? extends CompressionCodec>)
                  Class.forName("org.apache.hadoop.hive.ql.io.orc.LzoCodec");
          return lzo.newInstance();
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("LZO is not available.", e);
        } catch (InstantiationException e) {
          throw new IllegalArgumentException("Problem initializing LZO", e);
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Insufficient access to LZO", e);
        }
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }

  @Override
  public synchronized boolean checkMemory(double newScale) throws IOException {
    long limit = (long) Math.round(stripeSize * newScale);
    long size = estimateStripeSize();
    if (LOG.isDebugEnabled()) {
      LOG.debug("ORC writer " + path + " size = " + size + " limit = " +
                limit);
    }
    if (size > limit) {
      flushStripe();
      return true;
    }
    return false;
  }

  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   */
  private class BufferedStream implements OutStream.OutputReceiver {
    private final OutStream outStream;
    private final List<ByteBuffer> output = new ArrayList<ByteBuffer>();

    BufferedStream(String name, int bufferSize,
                   CompressionCodec codec) throws IOException {
      outStream = new OutStream(name, bufferSize, codec, this);
    }

    /**
     * Receive a buffer from the compression codec.
     * @param buffer the buffer to save
     * @throws IOException
     */
    @Override
    public void output(ByteBuffer buffer) {
      output.add(buffer);
    }

    /**
     * Get the number of bytes in buffers that are allocated to this stream.
     * @return number of bytes in buffers
     */
    public long getBufferSize() {
      long result = 0;
      for(ByteBuffer buf: output) {
        result += buf.capacity();
      }
      return outStream.getBufferSize() + result;
    }

    /**
     * Flush the stream to the codec.
     * @throws IOException
     */
    public void flush() throws IOException {
      outStream.flush();
    }

    /**
     * Clear all of the buffers.
     * @throws IOException
     */
    public void clear() throws IOException {
      outStream.clear();
      output.clear();
    }

    /**
     * Write the saved compressed buffers to the OutputStream.
     * @param out the stream to write to
     * @throws IOException
     */
    void spillTo(OutputStream out) throws IOException {
      for(ByteBuffer buffer: output) {
        out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
      }
    }

    @Override
    public String toString() {
      return outStream.toString();
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   */
  private class DirectStream implements OutStream.OutputReceiver {
    private final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        buffer.remaining());
    }
  }

  private static class RowIndexPositionRecorder implements PositionRecorder {
    private final OrcProto.RowIndexEntry.Builder builder;

    RowIndexPositionRecorder(OrcProto.RowIndexEntry.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void addPosition(long position) {
      builder.addPositions(position);
    }
  }

  /**
   * Interface from the Writer to the TreeWriters. This limits the visibility
   * that the TreeWriters have into the Writer.
   */
  private class StreamFactory {
    /**
     * Create a stream to store part of a column.
     * @param column the column id for the stream
     * @param kind the kind of stream
     * @return The output outStream that the section needs to be written to.
     * @throws IOException
     */
    public PositionedOutputStream createStream(int column,
                                               OrcProto.Stream.Kind kind
                                              ) throws IOException {
      StreamName name = new StreamName(column, kind);
      BufferedStream result = streams.get(name);
      if (result == null) {
        result = new BufferedStream(name.toString(), bufferSize, codec);
        streams.put(name, result);
      }
      return result.outStream;
    }

    /**
     * Get the next column id.
     * @return a number from 0 to the number of columns - 1
     */
    public int getNextColumnId() {
      return columnCount++;
    }

    /**
     * Get the stride rate of the row index.
     */
    public int getRowIndexStride() {
      return rowIndexStride;
    }

    /**
     * Should be building the row index.
     * @return true if we are building the index
     */
    public boolean buildIndex() {
      return buildIndex;
    }
  }

  /**
   * The parent class of all of the writers for each column. Each column
   * is written by an instance of this class. The compound types (struct,
   * list, map, and union) have children tree writers that write the children
   * types.
   */
  private abstract static class TreeWriter {
    protected final int id;
    protected final ObjectInspector inspector;
    private final BitFieldWriter isPresent;
    protected final ColumnStatisticsImpl indexStatistics;
    private final ColumnStatisticsImpl fileStatistics;
    protected TreeWriter[] childrenWriters;
    protected final RowIndexPositionRecorder rowIndexPosition;
    private final OrcProto.RowIndex.Builder rowIndex;
    private final OrcProto.RowIndexEntry.Builder rowIndexEntry;
    private final PositionedOutputStream rowIndexStream;
    private final Configuration conf;
    protected long stripeRawDataSize = 0;
    protected long rowRawDataSize = 0;
    protected final boolean useVInts;

    /**
     * Create a tree writer
     * @param columnId the column id of the column to write
     * @param inspector the object inspector to use
     * @param streamFactory limited access to the Writer's data.
     * @param nullable can the value be null?
     * @throws IOException
     */
    TreeWriter(int columnId, ObjectInspector inspector,
               StreamFactory streamFactory,
               boolean nullable, Configuration conf,
               boolean useVInts) throws IOException {
      this.id = columnId;
      this.inspector = inspector;
      this.conf = conf;
      this.useVInts = useVInts;
      if (nullable) {
        isPresent = new BitFieldWriter(streamFactory.createStream(id,
            OrcProto.Stream.Kind.PRESENT), 1);
      } else {
        isPresent = null;
      }
      indexStatistics = ColumnStatisticsImpl.create(inspector);
      fileStatistics = ColumnStatisticsImpl.create(inspector);
      childrenWriters = new TreeWriter[0];
      rowIndex = OrcProto.RowIndex.newBuilder();
      rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
      rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
      if (streamFactory.buildIndex()) {
        rowIndexStream = streamFactory.createStream(id,
            OrcProto.Stream.Kind.ROW_INDEX);
      } else {
        rowIndexStream = null;
      }
    }

    protected OrcProto.RowIndex.Builder getRowIndex() {
      return rowIndex;
    }

    protected ColumnStatisticsImpl getFileStatistics() {
      return fileStatistics;
    }

    protected OrcProto.RowIndexEntry.Builder getRowIndexEntry() {
      return rowIndexEntry;
    }

    /**
     * Add a new value to the column.
     * @param obj
     * @throws IOException
     */
    abstract void write(Object obj) throws IOException;

    void write(Object obj, long rawDataSize) throws IOException{
      if (obj != null) {
        setRawDataSize(rawDataSize);
      } else {
        // Estimate the raw size of null as 1 byte
        setRawDataSize(RawDatasizeConst.NULL_SIZE);
      }

      flushRow(obj);
    }

    /**
     * Update the row count and mark the isPresent bit
     */
    void flushRow(Object obj) throws IOException {
      if (obj != null) {
        indexStatistics.increment();
      }

      if (isPresent != null) {
        isPresent.write(obj == null ? 0 : 1);
      }
    }

    /**
     * Sets the row raw data size and updates the stripe raw data size
     *
     * @param rawDataSize
     */
    protected void setRawDataSize(long rawDataSize) {
      rowRawDataSize = rawDataSize;
      stripeRawDataSize += rawDataSize;
    }

    /**
     * Write the stripe out to the file.
     * @param builder the stripe footer that contains the information about the
     *                layout of the stripe. The TreeWriter is required to update
     *                the footer with its information.
     * @param requiredIndexEntries the number of index entries that are
     *                             required. this is to check to make sure the
     *                             row index is well formed.
     * @throws IOException
     */
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      if (isPresent != null) {
        isPresent.flush();
      }
      builder.addColumns(getEncoding());
      if (rowIndexStream != null) {
        if (rowIndex.getEntryCount() != requiredIndexEntries) {
          throw new IllegalArgumentException("Column has wrong number of " +
               "index entries found: " + rowIndexEntry + " expected: " +
               requiredIndexEntries);
        }
        rowIndex.build().writeTo(rowIndexStream);
        rowIndexStream.flush();
      }
      rowIndex.clear();
      rowIndexEntry.clear();
      stripeRawDataSize = 0;
    }

    TreeWriter[] getChildrenWriters() {
      return childrenWriters;
    }

    /**
     * For all the TreeWriters that buffer rows, process
     * all the buffered rows.
     */
    void flush() throws IOException {
      for (TreeWriter writer : childrenWriters) {
        writer.flush();
      }
      return;
    }

    /**
     * Get the encoding for this column.
     * @return the information about the encoding of this column
     */
    OrcProto.ColumnEncoding getEncoding() {
      return OrcProto.ColumnEncoding.newBuilder().setKind(
          OrcProto.ColumnEncoding.Kind.DIRECT).build();
    }

    /**
     * Create a row index entry with the previous location and the current
     * index statistics. Also merges the index statistics into the file
     * statistics before they are cleared. Finally, it records the start of the
     * next index and ensures all of the children columns also create an entry.
     * @throws IOException
     */
    void createRowIndexEntry() throws IOException {
      fileStatistics.merge(indexStatistics);
      rowIndexEntry.setStatistics(indexStatistics.serialize());
      indexStatistics.reset();
      rowIndex.addEntry(rowIndexEntry);
      rowIndexEntry.clear();
      recordPosition(rowIndexPosition);
      for(TreeWriter child: childrenWriters) {
        child.createRowIndexEntry();
      }
    }

    /**
     * Record the current position in each of this column's streams.
     * @param recorder where should the locations be recorded
     * @throws IOException
     */
    void recordPosition(PositionRecorder recorder) throws IOException {
      if (isPresent != null) {
        isPresent.getPosition(recorder);
      }
    }

    /**
     * Estimate how much memory the writer is consuming excluding the streams.
     * @return the number of bytes.
     */
    long estimateMemory() {
      long result = 0;
      for (TreeWriter child: childrenWriters) {
        result += child.estimateMemory();
      }
      return result;
    }

    long getStripeRawDataSize() {
      return stripeRawDataSize;
    }

    long getRowRawDataSize() {
      return rowRawDataSize;
    }
  }

  private static class BooleanTreeWriter extends TreeWriter {
    private final BitFieldWriter writer;

    BooleanTreeWriter(int columnId,
                      ObjectInspector inspector,
                      StreamFactory writer,
                      boolean nullable, Configuration conf,
                      boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      PositionedOutputStream out = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.writer = new BitFieldWriter(out, 1);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj, RawDatasizeConst.BOOLEAN_SIZE);
      if (obj != null) {
        boolean val = ((BooleanObjectInspector) inspector).get(obj);
        indexStatistics.updateBoolean(val);
        writer.write(val ? 1 : 0);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      writer.flush();
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      writer.getPosition(recorder);
    }
  }

  private static class ByteTreeWriter extends TreeWriter {
    private final RunLengthByteWriter writer;

    ByteTreeWriter(int columnId,
                      ObjectInspector inspector,
                      StreamFactory writer,
                      boolean nullable, Configuration conf,
                      boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      this.writer = new RunLengthByteWriter(writer.createStream(id,
          OrcProto.Stream.Kind.DATA));
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj, RawDatasizeConst.BYTE_SIZE);
      if (obj != null) {
        byte val = ((ByteObjectInspector) inspector).get(obj);
        indexStatistics.updateInteger(val);
        writer.write(val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      writer.flush();
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      writer.getPosition(recorder);
    }
  }

  private static class IntegerTreeWriter extends TreeWriter {
    private final PositionedOutputStream output;
    private final RunLengthIntegerWriter lengthOutput;
    private final DynamicIntArray rows = new DynamicIntArray();
    private final List<OrcProto.RowIndexEntry> savedRowIndex =
        new ArrayList<OrcProto.RowIndexEntry>();
    private final boolean buildIndex;
    private final List<Long> rowIndexValueCount = new ArrayList<Long>();
    private final float dictionaryKeySizeThreshold;

    private final IntDictionaryEncoder dictionary;
    private boolean useDictionaryEncoding = true;
    private final StreamFactory writer;
    private final int numBytes;
    private final Long[] buffer;
    private int bufferIndex = 0;
    private long bufferedBytes = 0;

    IntegerTreeWriter(int columnId,
                      ObjectInspector inspector,
                      StreamFactory writerFactory,
                      boolean nullable, Configuration conf,
                      boolean useVInts, int numBytes) throws IOException {
      super(columnId, inspector, writerFactory, nullable, conf, useVInts);
      writer = writerFactory;
      final boolean sortKeys = conf.getBoolean(
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS.varname,
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS.defaultBoolVal);

      this.numBytes = numBytes;

      dictionary = new IntDictionaryEncoder(sortKeys, numBytes, useVInts);
      output = writer.createStream(id,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      lengthOutput = new RunLengthIntegerWriter(
          writer.createStream(id, OrcProto.Stream.Kind.LENGTH), false, INT_BYTE_SIZE, useVInts);

      dictionaryKeySizeThreshold = conf.getFloat(
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD.varname,
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD.defaultFloatVal);

      int bufferLength = conf.getInt(HiveConf.ConfVars.HIVE_ORC_ROW_BUFFER_SIZE.varname,
          HiveConf.ConfVars.HIVE_ORC_ROW_BUFFER_SIZE.defaultIntVal);
      buffer = new Long[bufferLength];

      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(0L);
      buildIndex = writer.buildIndex();
    }

    @Override
    void write(Object obj) throws IOException {
      if (obj != null) {
        switch (inspector.getCategory()) {
          case PRIMITIVE:
            switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
              case SHORT:
                buffer[bufferIndex++] = new Long(((ShortObjectInspector) inspector).get(obj));
                setRawDataSize(RawDatasizeConst.SHORT_SIZE);
                break;
              case INT:
                buffer[bufferIndex++] = new Long(((IntObjectInspector) inspector).get(obj));
                setRawDataSize(RawDatasizeConst.INT_SIZE);
                break;
              case LONG:
                buffer[bufferIndex++] = new Long(((LongObjectInspector) inspector).get(obj));
                setRawDataSize(RawDatasizeConst.LONG_SIZE);
                break;
              default:
                throw new IllegalArgumentException("Bad Category: Dictionary Encoding not available for " +
                    ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
            }
            break;
          default:
            throw new IllegalArgumentException("Bad Category: DictionaryEncoding not available for " + inspector.getCategory());
        }
        bufferedBytes += RawDatasizeConst.LONG_SIZE;
      } else {
        buffer[bufferIndex++] = null;
        setRawDataSize(RawDatasizeConst.NULL_SIZE);
      }
      if (bufferIndex == buffer.length) {
        flush();
      }
    }

    @Override
    void flush() throws IOException {
      for (int i = 0; i < bufferIndex; i++) {
        Long val = buffer[i];
        buffer[i] = null;
        if (val != null) {
          rows.add(dictionary.add(val));
          indexStatistics.updateInteger(val);
        }
        super.flushRow(val);
      }
      bufferIndex = 0;
      bufferedBytes = 0;
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      final PositionedOutputStream rowOutput;
      if (rows.size() > 0 &&
          (float)(dictionary.size()) / (float)rows.size() <= dictionaryKeySizeThreshold) {
        useDictionaryEncoding = true;
        rowOutput =
          new RunLengthIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.DATA), false, INT_BYTE_SIZE, useVInts);
      } else {
        useDictionaryEncoding = false;
        rowOutput = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      }

      final int[] dumpOrder;
      if (useDictionaryEncoding) {
        // Traverse the dictionary keys writing out the bytes and lengths; and
        // creating the map from the original order to the final sorted order.
        dumpOrder = new int[dictionary.size()];
        dictionary.visit(new IntDictionaryEncoder.Visitor<Long>() {
          int currentId = 0;
          public void visit(IntDictionaryEncoder.VisitorContext<Long> context) throws IOException {
            context.writeBytes(output);
            dumpOrder[context.getOriginalPosition()] = currentId++;
          }
        });
      } else {
        dumpOrder = null;
      }

      int length = rows.size();
      int rowIndexEntry = 0;
      OrcProto.RowIndex.Builder rowIndex = getRowIndex();
      // need to build the first index entry out here, to handle the case of
      // not having any values.
      if (buildIndex) {
        while (0 == rowIndexValueCount.get(rowIndexEntry) &&
            rowIndexEntry < savedRowIndex.size()) {
          OrcProto.RowIndexEntry.Builder base =
              savedRowIndex.get(rowIndexEntry++).toBuilder();
          rowOutput.getPosition(new RowIndexPositionRecorder(base));
          rowIndex.addEntry(base.build());
        }
      }
      // write the values translated into the dump order.
      for(int i = 0; i <= length; ++i) {
        // now that we are writing out the row values, we can finalize the
        // row index
        if (buildIndex) {
          while (i == rowIndexValueCount.get(rowIndexEntry) &&
              rowIndexEntry < savedRowIndex.size()) {
            OrcProto.RowIndexEntry.Builder base =
                savedRowIndex.get(rowIndexEntry++).toBuilder();
            rowOutput.getPosition(new RowIndexPositionRecorder(base));
            rowIndex.addEntry(base.build());
          }
        }

        if (i < length) {
          if (useDictionaryEncoding && dumpOrder != null) {
            rowOutput.write(dumpOrder[rows.get(i)]);
          } else {
            SerializationUtils.writeIntegerType(rowOutput,
                dictionary.getValue(rows.get(i)), numBytes, true, useVInts);
          }
        }
      }
      // we need to build the rowindex before calling super, since it
      // writes it out.
      super.writeStripe(builder, requiredIndexEntries);
      if (useDictionaryEncoding) {
        output.flush();
        lengthOutput.flush();
      }
      rowOutput.flush();
      dictionary.clear();
      rows.clear();
      savedRowIndex.clear();
      rowIndexValueCount.clear();
      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(0L);
    }

    @Override
    OrcProto.ColumnEncoding getEncoding() {
      if (useDictionaryEncoding) {
        return OrcProto.ColumnEncoding.newBuilder().setKind(
            OrcProto.ColumnEncoding.Kind.DICTIONARY).
            setDictionarySize(dictionary.size()).build();
      } else {
        return OrcProto.ColumnEncoding.newBuilder().setKind(
            OrcProto.ColumnEncoding.Kind.DIRECT).build();
      }
    }

    /**
     * This method doesn't call the super method, because unlike most of the
     * other TreeWriters, this one can't record the position in the streams
     * until the stripe is being flushed. Therefore it saves all of the entries
     * and augments them with the final information as the stripe is written.
     * @throws IOException
     */
    @Override
    void createRowIndexEntry() throws IOException {
      getFileStatistics().merge(indexStatistics);
      OrcProto.RowIndexEntry.Builder rowIndexEntry = getRowIndexEntry();
      rowIndexEntry.setStatistics(indexStatistics.serialize());
      indexStatistics.reset();
      savedRowIndex.add(rowIndexEntry.build());
      rowIndexEntry.clear();
      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(Long.valueOf(rows.size()));
    }

    @Override
    long estimateMemory() {
      return rows.size() * 4 + dictionary.getByteSize() + bufferedBytes;
    }

  }

  private static class FloatTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;

    FloatTreeWriter(int columnId,
                      ObjectInspector inspector,
                      StreamFactory writer,
                      boolean nullable, Configuration conf,
                      boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      this.stream = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj, RawDatasizeConst.FLOAT_SIZE);
      if (obj != null) {
        float val = ((FloatObjectInspector) inspector).get(obj);
        indexStatistics.updateDouble(val);
        SerializationUtils.writeFloat(stream, val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      stream.flush();
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      stream.getPosition(recorder);
    }
  }

  private static class DoubleTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;

    DoubleTreeWriter(int columnId,
                    ObjectInspector inspector,
                    StreamFactory writer,
                    boolean nullable, Configuration conf,
                    boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      this.stream = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      super.write(obj, RawDatasizeConst.DOUBLE_SIZE);
      if (obj != null) {
        double val = ((DoubleObjectInspector) inspector).get(obj);
        indexStatistics.updateDouble(val);
        SerializationUtils.writeDouble(stream, val);
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      stream.flush();
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      stream.getPosition(recorder);
    }
  }

  private static class StringTreeWriter extends TreeWriter {
    private final PositionedOutputStream stringOutput;
    private final RunLengthIntegerWriter lengthOutput;
    private final StringDictionaryEncoder dictionary;
    private final DynamicIntArray rows = new DynamicIntArray();
    private final RunLengthIntegerWriter directLengthOutput;
    private final List<OrcProto.RowIndexEntry> savedRowIndex =
        new ArrayList<OrcProto.RowIndexEntry>();
    private final boolean buildIndex;
    private final List<Long> rowIndexValueCount = new ArrayList<Long>();
    private final StreamFactory writer;
    // If the number of keys in a dictionary is greater than this fraction of the total number of
    // non-null rows, turn off dictionary encoding
    private final float dictionaryKeySizeThreshold;
    // If the number of keys in a dictionary is greater than this fraction of the total number of
    // non-null rows, don't use the estimated entropy heuristic to turn off dictionary encoding
    private final float entropyKeySizeThreshold;
    private final int entropyMinSamples;
    private final float entropyDictSampleFraction;
    private final int entropyThreshold;

    private boolean useDictionaryEncoding = true;

    private final Text[] buffer;
    private int bufferIndex = 0;
    private long bufferedBytes = 0;

    StringTreeWriter(int columnId,
                     ObjectInspector inspector,
                     StreamFactory writerFactory,
                     boolean nullable, Configuration conf,
                     boolean useVInts) throws IOException {
      super(columnId, inspector, writerFactory, nullable, conf, useVInts);
      writer = writerFactory;
      final boolean sortKeys = conf.getBoolean(
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS.varname,
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS.defaultBoolVal);

      dictionary = new StringDictionaryEncoder(sortKeys);
      stringOutput = writer.createStream(id,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      lengthOutput = new RunLengthIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.LENGTH), false, INT_BYTE_SIZE, useVInts);
      directLengthOutput = new RunLengthIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.LENGTH), false, INT_BYTE_SIZE, useVInts);
      dictionaryKeySizeThreshold = conf.getFloat(
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD.varname,
          HiveConf.ConfVars.HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD.defaultFloatVal);
      entropyKeySizeThreshold = conf.getFloat(
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_KEY_STRING_SIZE_THRESHOLD.varname,
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_KEY_STRING_SIZE_THRESHOLD.defaultFloatVal);
      entropyMinSamples = conf.getInt(
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_MIN_SAMPLES.varname,
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_MIN_SAMPLES.defaultIntVal);
      entropyDictSampleFraction = conf.getFloat(
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_DICT_SAMPLE_FRACTION.varname,
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_DICT_SAMPLE_FRACTION.defaultFloatVal);
      entropyThreshold = conf.getInt(
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD.varname,
          HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD.defaultIntVal);

      int bufferLength = conf.getInt(HiveConf.ConfVars.HIVE_ORC_ROW_BUFFER_SIZE.varname,
          HiveConf.ConfVars.HIVE_ORC_ROW_BUFFER_SIZE.defaultIntVal);
      buffer = new Text[bufferLength];

      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(0L);
      buildIndex = writer.buildIndex();
    }

    @Override
    void write(Object obj) throws IOException {
      if (obj != null) {
        Text val = ((StringObjectInspector) inspector).getPrimitiveWritableObject(obj);
        buffer[bufferIndex++] = new Text(val);
        setRawDataSize(val.getLength());
        bufferedBytes += val.getLength();
      } else {
        buffer[bufferIndex++] = null;
        setRawDataSize(RawDatasizeConst.NULL_SIZE);
      }
      if (bufferIndex == buffer.length) {
        flush();
      }
    }

    @Override
    void flush() throws IOException {
      for (int i = 0; i < bufferIndex; i++) {
        Text val = buffer[i];
        // Make sure we don't end up storing it twice
        buffer[i] = null;
        if (val != null) {
          rows.add(dictionary.add(val));
          indexStatistics.updateString(val.toString());
        }
        super.flushRow(val);
      }
      bufferIndex = 0;
      bufferedBytes = 0;
    }

    private boolean isEntropyThresholdExceeded(Set<Character> chars, Text text, int index) {
      dictionary.getText(text, index);
      for (char character : text.toString().toCharArray()) {
        chars.add(character);
      }
      return chars.size() > entropyThreshold;
    }

    private int[] getSampleIndecesForEntropy() {
      int numSamples = Math.max(entropyMinSamples,
          (int)(entropyDictSampleFraction * dictionary.size()));
      int[] indeces = new int[dictionary.size()];
      int[] samples = new int[numSamples];
      Random rand = new Random();

      // The goal of this loop is to select numSamples number of distinct indeces of
      // dictionary
      //
      // The loop works as follows, start with an array of zeros
      // On each iteration pick a random number in the range of 0 to size of the dictionary
      // minus one minus the number of previous iterations, thus the effective size of the
      // array is decreased by one with each iteration (not actually but logically)
      // Look at the value of the array at that random index, if it is 0, the sample index is
      // the random index because we've never looked at this position before, if it's
      // nonzero, that value is the sample index
      // Then take the value at the end of the logical size of the array (size of the
      // dictionary minus one minus the number of iterations) if it's 0 put that index into
      // the array at the random index, otherwise put the nonzero value
      // Thus, by reducing the logical size of the array and moving the value at the end of
      // the array we are removing indexes we have previously visited and making sure we do
      // not lose any indexes
      for (int i = 0; i < numSamples; i++) {
        int index = rand.nextInt(dictionary.size() - i);
        if (indeces[index] == 0) {
          samples[i] = index;
          } else {
            samples[i] = indeces[index];
        }
        indeces[index] = indeces[dictionary.size() - i - 1] == 0 ?
            dictionary.size() - i - 1 : indeces[dictionary.size() - i - 1];
      }

      return samples;
    }

    private boolean useDictionaryEncodingEntropyHeuristic() {
      Set<Character> chars = new HashSet<Character>();
      Text text = new Text();
      if (dictionary.size() > entropyMinSamples) {

        int[] samples = getSampleIndecesForEntropy();

        for (int sampleIndex : samples) {
          if (isEntropyThresholdExceeded(chars, text, sampleIndex)) {
            return true;
          }
        }
      } else {
        for (int i = 0; i < dictionary.size(); i++) {
          if (isEntropyThresholdExceeded(chars, text, i)) {
            return true;
          }
        }
      }

      return false;
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      final PositionedOutputStream rowOutput;
      // Set the flag indicating whether or not to use dictionary encoding based on whether
      // or not the fraction of distinct keys over number of non-null rows is less than the
      // configured threshold, and whether or not the number of distinct characters in a sample
      // of entries in the dictionary (the estimated entropy) exceeds the configured threshold
      if (rows.size() > 0) {
        useDictionaryEncoding = true;
        // The fraction of non-null values in this column that are repeats of values in the
        // dictionary
        float repeatedValuesFraction =
          (float)(rows.size() - dictionary.size()) / (float)rows.size();

        // If the number of repeated values is small enough, consider using the entropy heuristic
        // If the number of repeated values is high, even in the presence of low entropy,
        // dictionary encoding can provide benefits beyond just zlib
        if (repeatedValuesFraction <= entropyKeySizeThreshold) {
          useDictionaryEncoding = useDictionaryEncodingEntropyHeuristic();
        }

        // dictionaryKeySizeThreshold is the fraction of keys that are distinct beyond
        // which dictionary encoding is turned off
        // so 1 - dictionaryKeySizeThreshold is the number of repeated values below which
        // dictionary encoding should be turned off
        useDictionaryEncoding = useDictionaryEncoding && (repeatedValuesFraction > 1.0 - dictionaryKeySizeThreshold);
      }

      if (useDictionaryEncoding) {
        rowOutput = new RunLengthIntegerWriter(writer.createStream(id,
            OrcProto.Stream.Kind.DATA), false, INT_BYTE_SIZE, useVInts);
      } else {
        rowOutput = writer.createStream(id,
            OrcProto.Stream.Kind.DATA);
      }

      final int[] dumpOrder = new int[dictionary.size()];

      if (useDictionaryEncoding) {
        // Traverse the red-black tree writing out the bytes and lengths; and
        // creating the map from the original order to the final sorted order.
        dictionary.visit(new StringDictionaryEncoder.Visitor<Text>() {
          private int currentId = 0;
          @Override
          public void visit(StringDictionaryEncoder.VisitorContext<Text> context
                           ) throws IOException {
            context.writeBytes(stringOutput);
            lengthOutput.write(context.getLength());
            dumpOrder[context.getOriginalPosition()] = currentId++;
          }
        });
      }

      int length = rows.size();
      int rowIndexEntry = 0;
      OrcProto.RowIndex.Builder rowIndex = getRowIndex();

      Text text = new Text();
      // write the values translated into the dump order.
      for(int i = 0; i <= length; ++i) {
        // now that we are writing out the row values, we can finalize the
        // row index
        if (buildIndex) {
          while (i == rowIndexValueCount.get(rowIndexEntry) &&
              rowIndexEntry < savedRowIndex.size()) {
            OrcProto.RowIndexEntry.Builder base =
                savedRowIndex.get(rowIndexEntry++).toBuilder();
            recordOutputPosition(rowOutput, base);
            rowIndex.addEntry(base.build());
          }
        }
        if (i !=  length) {
          if (useDictionaryEncoding) {
            rowOutput.write(dumpOrder[rows.get(i)]);
          } else {
            dictionary.getText(text, rows.get(i));
            rowOutput.write(text.getBytes(), 0, text.getLength());
            directLengthOutput.write(text.getLength());
          }
        }
      }
      // we need to build the rowindex before calling super, since it
      // writes it out.
      super.writeStripe(builder, requiredIndexEntries);
      rowOutput.flush();
      if (useDictionaryEncoding) {
        stringOutput.flush();
        lengthOutput.flush();
      } else {
        directLengthOutput.flush();
      }

      // reset all of the fields to be ready for the next stripe.
      dictionary.clear();
      rows.clear();
      savedRowIndex.clear();
      rowIndexValueCount.clear();
      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(0L);
    }

    // Calls getPosition on the row output stream if dictionary encoding is used, and the direct
    // output stream if direct encoding is used
    private void recordOutputPosition(PositionedOutputStream rowOutput, OrcProto.RowIndexEntry.Builder base) throws IOException {
      rowOutput.getPosition(new RowIndexPositionRecorder(base));
      if (!useDictionaryEncoding) {
        directLengthOutput.getPosition(new RowIndexPositionRecorder(base));
      }
    }

    @Override
    OrcProto.ColumnEncoding getEncoding() {
      // Returns the encoding used for the last call to writeStripe
      if (useDictionaryEncoding) {
        return OrcProto.ColumnEncoding.newBuilder().setKind(
            OrcProto.ColumnEncoding.Kind.DICTIONARY).
            setDictionarySize(dictionary.size()).build();
      } else {
        return OrcProto.ColumnEncoding.newBuilder().setKind(
            OrcProto.ColumnEncoding.Kind.DIRECT).build();
      }
    }

    /**
     * This method doesn't call the super method, because unlike most of the
     * other TreeWriters, this one can't record the position in the streams
     * until the stripe is being flushed. Therefore it saves all of the entries
     * and augments them with the final information as the stripe is written.
     * @throws IOException
     */
    @Override
    void createRowIndexEntry() throws IOException {
      getFileStatistics().merge(indexStatistics);
      OrcProto.RowIndexEntry.Builder rowIndexEntry = getRowIndexEntry();
      rowIndexEntry.setStatistics(indexStatistics.serialize());
      indexStatistics.reset();
      savedRowIndex.add(rowIndexEntry.build());
      rowIndexEntry.clear();
      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(Long.valueOf(rows.size()));
    }

    @Override
    long estimateMemory() {
      return rows.getSizeInBytes() + dictionary.getSizeInBytes() + bufferedBytes;
    }
  }

  private static class BinaryTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;
    private final RunLengthIntegerWriter length;

    BinaryTreeWriter(int columnId,
                     ObjectInspector inspector,
                     StreamFactory writer,
                     boolean nullable, Configuration conf,
                     boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      this.stream = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.length = new RunLengthIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.LENGTH), false, INT_BYTE_SIZE, useVInts);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      long rawDataSize = 0;
      if (obj != null) {
        BytesWritable val =
            ((BinaryObjectInspector) inspector).getPrimitiveWritableObject(obj);
        stream.write(val.getBytes(), 0, val.getLength());
        length.write(val.getLength());

        // Raw data size is the length of the BytesWritable, i.e. the number of bytes
        rawDataSize = val.getLength();
      }
      super.write(obj, rawDataSize);
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      stream.flush();
      length.flush();
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      stream.getPosition(recorder);
      length.getPosition(recorder);
    }
  }

  static final int MILLIS_PER_SECOND = 1000;
  static final long BASE_TIMESTAMP =
      Timestamp.valueOf("2015-01-01 00:00:00").getTime() / MILLIS_PER_SECOND;

  private static class TimestampTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter seconds;
    private final RunLengthIntegerWriter nanos;

    TimestampTreeWriter(int columnId,
                     ObjectInspector inspector,
                     StreamFactory writer,
                     boolean nullable, Configuration conf,
                     boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      this.seconds = new RunLengthIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.DATA), true, LONG_BYTE_SIZE, useVInts);
      this.nanos = new RunLengthIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.NANO_DATA), false, LONG_BYTE_SIZE, useVInts);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      // Raw data size is:
      //   the number of bytes needed to store the milliseconds since the epoch
      //   (8 since it's a long)
      //   +
      //   the number of bytes needed to store the nanos field (4 since it's an int)
      super.write(obj, RawDatasizeConst.TIMESTAMP_SIZE);
      if (obj != null) {
        Timestamp val =
            ((TimestampObjectInspector) inspector).
                getPrimitiveJavaObject(obj);
        seconds.write((val.getTime() / MILLIS_PER_SECOND) - BASE_TIMESTAMP);
        nanos.write(formatNanos(val.getNanos()));
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      seconds.flush();
      nanos.flush();
      recordPosition(rowIndexPosition);
    }

    private static long formatNanos(int nanos) {
      if (nanos == 0) {
        return 0;
      } else if (nanos % 100 != 0) {
        return ((long) nanos) << 3;
      } else {
        nanos /= 100;
        int trailingZeros = 1;
        while (nanos % 10 == 0 && trailingZeros < 7) {
          nanos /= 10;
          trailingZeros += 1;
        }
        return ((long) nanos) << 3 | trailingZeros;
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      seconds.getPosition(recorder);
      nanos.getPosition(recorder);
    }
  }

  private static class StructTreeWriter extends TreeWriter {
    private final List<? extends StructField> fields;
    StructTreeWriter(int columnId,
                     ObjectInspector inspector,
                     StreamFactory writer,
                     boolean nullable, Configuration conf,
                     boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      StructObjectInspector structObjectInspector =
        (StructObjectInspector) inspector;
      fields = structObjectInspector.getAllStructFieldRefs();
      childrenWriters = new TreeWriter[fields.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] = createTreeWriter(
          fields.get(i).getFieldObjectInspector(), writer, true, conf, useVInts);
      }
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      long rawDataSize = 0;
      if (obj != null) {
        StructObjectInspector insp = (StructObjectInspector) inspector;
        for(int i = 0; i < fields.size(); ++i) {
          StructField field = fields.get(i);
          TreeWriter writer = childrenWriters[i];
          writer.write(insp.getStructFieldData(obj, field));
          rawDataSize += writer.getRowRawDataSize();
        }
      }
      super.write(obj, rawDataSize);
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      recordPosition(rowIndexPosition);
    }
  }

  private static class ListTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter lengths;

    ListTreeWriter(int columnId,
                   ObjectInspector inspector,
                   StreamFactory writer,
                   boolean nullable, Configuration conf,
                   boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      ListObjectInspector listObjectInspector = (ListObjectInspector) inspector;
      childrenWriters = new TreeWriter[1];
      childrenWriters[0] =
        createTreeWriter(listObjectInspector.getListElementObjectInspector(),
          writer, true, conf, useVInts);
      lengths =
        new RunLengthIntegerWriter(writer.createStream(columnId,
            OrcProto.Stream.Kind.LENGTH), false, INT_BYTE_SIZE, useVInts);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      long rawDataSize = 0;
      if (obj != null) {
        ListObjectInspector insp = (ListObjectInspector) inspector;
        int len = insp.getListLength(obj);
        lengths.write(len);
        for(int i=0; i < len; ++i) {
          childrenWriters[0].write(insp.getListElement(obj, i));
          rawDataSize += childrenWriters[0].getRowRawDataSize();
        }
      }
      super.write(obj, rawDataSize);
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      lengths.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      lengths.getPosition(recorder);
    }
  }

  private static class MapTreeWriter extends TreeWriter {
    private final RunLengthIntegerWriter lengths;

    MapTreeWriter(int columnId,
                  ObjectInspector inspector,
                  StreamFactory writer,
                  boolean nullable, Configuration conf,
                  boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      MapObjectInspector insp = (MapObjectInspector) inspector;
      childrenWriters = new TreeWriter[2];
      childrenWriters[0] =
        createTreeWriter(insp.getMapKeyObjectInspector(), writer, true, conf, useVInts);
      childrenWriters[1] =
        createTreeWriter(insp.getMapValueObjectInspector(), writer, true, conf, useVInts);
      lengths =
        new RunLengthIntegerWriter(writer.createStream(columnId,
            OrcProto.Stream.Kind.LENGTH), false, INT_BYTE_SIZE, useVInts);
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      long rawDataSize = 0;
      if (obj != null) {
        MapObjectInspector insp = (MapObjectInspector) inspector;
        int len = insp.getMapSize(obj);
        lengths.write(len);
        // this sucks, but it will have to do until we can get a better
        // accessor in the MapObjectInspector.
        Map<?, ?> valueMap = insp.getMap(obj);
        for(Map.Entry<?, ?> entry: valueMap.entrySet()) {
          childrenWriters[0].write(entry.getKey());
          childrenWriters[1].write(entry.getValue());
          rawDataSize += childrenWriters[0].getRowRawDataSize();
          rawDataSize += childrenWriters[1].getRowRawDataSize();
        }
      }
      super.write(obj, rawDataSize);
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      lengths.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      lengths.getPosition(recorder);
    }
  }

  private static class UnionTreeWriter extends TreeWriter {
    private final RunLengthByteWriter tags;

    UnionTreeWriter(int columnId,
                  ObjectInspector inspector,
                  StreamFactory writer,
                  boolean nullable, Configuration conf,
                  boolean useVInts) throws IOException {
      super(columnId, inspector, writer, nullable, conf, useVInts);
      UnionObjectInspector insp = (UnionObjectInspector) inspector;
      List<ObjectInspector> choices = insp.getObjectInspectors();
      childrenWriters = new TreeWriter[choices.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] = createTreeWriter(choices.get(i), writer, true, conf, useVInts);
      }
      tags =
        new RunLengthByteWriter(writer.createStream(columnId,
            OrcProto.Stream.Kind.DATA));
      recordPosition(rowIndexPosition);
    }

    @Override
    void write(Object obj) throws IOException {
      long rawDataSize = 0;
      if (obj != null) {
        UnionObjectInspector insp = (UnionObjectInspector) inspector;
        byte tag = insp.getTag(obj);
        tags.write(tag);
        childrenWriters[tag].write(insp.getField(obj));
        // raw data size is size of tag (1) + size of value
        rawDataSize = childrenWriters[tag].getRowRawDataSize() + RawDatasizeConst.UNION_TAG_SIZE;
      }
      super.write(obj, rawDataSize);
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      tags.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      recordPosition(rowIndexPosition);
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      tags.getPosition(recorder);
    }
  }

  private static TreeWriter createTreeWriter(ObjectInspector inspector,
                                             StreamFactory streamFactory,
                                             boolean nullable, Configuration conf,
                                             boolean useVInts) throws IOException {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
          case BOOLEAN:
            return new BooleanTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          case BYTE:
            return new ByteTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          case SHORT:
            return new IntegerTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts , SHORT_BYTE_SIZE);
          case INT:
            return new IntegerTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts , INT_BYTE_SIZE);
          case LONG:
            return new IntegerTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts , LONG_BYTE_SIZE);
          case FLOAT:
            return new FloatTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          case DOUBLE:
            return new DoubleTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          case STRING:
            return new StringTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          case BINARY:
            return new BinaryTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          case TIMESTAMP:
            return new TimestampTreeWriter(streamFactory.getNextColumnId(),
                inspector, streamFactory, nullable, conf, useVInts);
          default:
            throw new IllegalArgumentException("Bad primitive category " +
              ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
        }
      case STRUCT:
        return new StructTreeWriter(streamFactory.getNextColumnId(), inspector,
            streamFactory, nullable, conf, useVInts);
      case MAP:
        return new MapTreeWriter(streamFactory.getNextColumnId(), inspector,
            streamFactory, nullable, conf, useVInts);
      case LIST:
        return new ListTreeWriter(streamFactory.getNextColumnId(), inspector,
            streamFactory, nullable, conf, useVInts);
      case UNION:
        return new UnionTreeWriter(streamFactory.getNextColumnId(), inspector,
            streamFactory, nullable, conf, useVInts);
      default:
        throw new IllegalArgumentException("Bad category: " +
          inspector.getCategory());
    }
  }

  private static void writeTypes(OrcProto.Footer.Builder builder,
                                 TreeWriter treeWriter) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    switch (treeWriter.inspector.getCategory()) {
      case PRIMITIVE:
        switch (((PrimitiveObjectInspector) treeWriter.inspector).
                 getPrimitiveCategory()) {
          case BOOLEAN:
            type.setKind(OrcProto.Type.Kind.BOOLEAN);
            break;
          case BYTE:
            type.setKind(OrcProto.Type.Kind.BYTE);
            break;
          case SHORT:
            type.setKind(OrcProto.Type.Kind.SHORT);
            break;
          case INT:
            type.setKind(OrcProto.Type.Kind.INT);
            break;
          case LONG:
            type.setKind(OrcProto.Type.Kind.LONG);
            break;
          case FLOAT:
            type.setKind(OrcProto.Type.Kind.FLOAT);
            break;
          case DOUBLE:
            type.setKind(OrcProto.Type.Kind.DOUBLE);
            break;
          case STRING:
            type.setKind(OrcProto.Type.Kind.STRING);
            break;
          case BINARY:
            type.setKind(OrcProto.Type.Kind.BINARY);
            break;
          case TIMESTAMP:
            type.setKind(OrcProto.Type.Kind.TIMESTAMP);
            break;
          default:
            throw new IllegalArgumentException("Unknown primitive category: " +
              ((PrimitiveObjectInspector) treeWriter.inspector).
                getPrimitiveCategory());
        }
        break;
      case LIST:
        type.setKind(OrcProto.Type.Kind.LIST);
        type.addSubtypes(treeWriter.childrenWriters[0].id);
        break;
      case MAP:
        type.setKind(OrcProto.Type.Kind.MAP);
        type.addSubtypes(treeWriter.childrenWriters[0].id);
        type.addSubtypes(treeWriter.childrenWriters[1].id);
        break;
      case STRUCT:
        type.setKind(OrcProto.Type.Kind.STRUCT);
        for(TreeWriter child: treeWriter.childrenWriters) {
          type.addSubtypes(child.id);
        }
        for(StructField field: ((StructTreeWriter) treeWriter).fields) {
          type.addFieldNames(field.getFieldName());
        }
        break;
      case UNION:
        type.setKind(OrcProto.Type.Kind.UNION);
        for(TreeWriter child: treeWriter.childrenWriters) {
          type.addSubtypes(child.id);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " +
          treeWriter.inspector.getCategory());
    }
    builder.addTypes(type);
    for(TreeWriter child: treeWriter.childrenWriters) {
      writeTypes(builder, child);
    }
  }

  private void ensureWriter() throws IOException {
    if (rawWriter == null) {
      rawWriter = fs.create(path, false, HDFS_BUFFER_SIZE,
        fs.getDefaultReplication(),
          // Use the largest value less than or equal to Integer.MAX_VALUE which is divisible
          // by dfsBytesPerChecksum as an upper bound (otherwise HDFS will throw an exception)
          Math.min(stripeSize * 2L, (Integer.MAX_VALUE / dfsBytesPerChecksum) * dfsBytesPerChecksum));
      rawWriter.writeBytes(OrcFile.MAGIC);
      headerLength = rawWriter.getPos();
      writer = new OutStream("metadata", bufferSize, codec,
        new DirectStream(rawWriter));
      protobufWriter = CodedOutputStream.newInstance(writer);
    }
  }

  private void createRowIndexEntry() throws IOException {
    treeWriter.flush();
    treeWriter.createRowIndexEntry();
    rowsInIndex = 0;
  }

  void addStripe(StripeInformation si, byte[] data) throws IOException {
    ensureWriter();
    OrcProto.StripeInformation dirEntry =
      OrcProto.StripeInformation.newBuilder()
          .setOffset(rawWriter.getPos())
          .setIndexLength(si.getIndexLength())
          .setDataLength(si.getDataLength())
          .setNumberOfRows(si.getNumberOfRows())
          .setFooterLength(si.getFooterLength()).build();
    stripes.add(dirEntry);
    rowCount += si.getNumberOfRows();
    rawWriter.write(data);
  }

  private void flushStripe() throws IOException {
    ensureWriter();
    treeWriter.flush();
    if (buildIndex && rowsInIndex != 0) {
      createRowIndexEntry();
    }
    if (rowsInStripe != 0) {
      int requiredIndexEntries = rowIndexStride == 0 ? 0 :
          (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
      OrcProto.StripeFooter.Builder builder =
          OrcProto.StripeFooter.newBuilder();
      long stripeRawDataSize = treeWriter.getStripeRawDataSize();
      treeWriter.writeStripe(builder, requiredIndexEntries);
      long start = rawWriter.getPos();
      long section = start;
      long indexEnd = start;
      for(Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
        BufferedStream stream = pair.getValue();
        stream.flush();
        stream.spillTo(rawWriter);
        stream.clear();
        long end = rawWriter.getPos();
        StreamName name = pair.getKey();
        builder.addStreams(OrcProto.Stream.newBuilder()
            .setColumn(name.getColumn())
            .setKind(name.getKind())
            .setLength(end-section)
            .setUseVInts(useVInts));
        section = end;
        if (StreamName.Area.INDEX == name.getArea()) {
          indexEnd = end;
        }
      }
      builder.build().writeTo(protobufWriter);
      protobufWriter.flush();
      writer.flush();
      long end = rawWriter.getPos();
      OrcProto.StripeInformation dirEntry =
          OrcProto.StripeInformation.newBuilder()
              .setOffset(start)
              .setIndexLength(indexEnd - start)
              .setDataLength(section - indexEnd)
              .setNumberOfRows(rowsInStripe)
              .setFooterLength(end - section)
              .setRawDataSize(stripeRawDataSize).build();
      stripes.add(dirEntry);
      rowCount += rowsInStripe;
      rawDataSize += stripeRawDataSize;
      rowsInStripe = 0;
    }
  }

  private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
    switch (kind) {
      case NONE: return OrcProto.CompressionKind.NONE;
      case ZLIB: return OrcProto.CompressionKind.ZLIB;
      case SNAPPY: return OrcProto.CompressionKind.SNAPPY;
      case LZO: return OrcProto.CompressionKind.LZO;
      default:
        throw new IllegalArgumentException("Unknown compression " + kind);
    }
  }

  private int writeFileStatistics(OrcProto.Footer.Builder builder,
      TreeWriter writer, ColumnStatisticsImpl[] columnStats, int column) {
    if (columnStats != null) {
      writer.fileStatistics.merge(columnStats[column++]);
    }
    builder.addStatistics(writer.fileStatistics.serialize());
    for(TreeWriter child: writer.getChildrenWriters()) {
      column = writeFileStatistics(builder, child, columnStats, column);
    }
    return column;
  }

  private int writeFooter(long bodyLength, ColumnStatisticsImpl[] columnStats) throws IOException {
    ensureWriter();
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    builder.setNumberOfRows(rowCount);
    builder.setRawDataSize(rawDataSize);
    builder.setRowIndexStride(rowIndexStride);
    // serialize the types
    writeTypes(builder, treeWriter);
    // add the stripe information
    for(OrcProto.StripeInformation stripe: stripes) {
      builder.addStripes(stripe);
    }
    // add the column statistics
    writeFileStatistics(builder, treeWriter, columnStats, 0);
    // add all of the user metadata
    for(Map.Entry<String, ByteString> entry: userMetadata.entrySet()) {
      builder.addMetadata(OrcProto.UserMetadataItem.newBuilder()
        .setName(entry.getKey()).setValue(entry.getValue()));
    }
    long startPosn = rawWriter.getPos();
    builder.build().writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    return (int) (rawWriter.getPos() - startPosn);
  }

  private int writePostScript(int footerLength) throws IOException {
    OrcProto.PostScript.Builder builder =
      OrcProto.PostScript.newBuilder()
        .setCompression(writeCompressionKind(compress))
        .setFooterLength(footerLength);
    if (compress != CompressionKind.NONE) {
      builder.setCompressionBlockSize(bufferSize);
    }
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    return (int) length;
  }

  private long estimateStripeSize() {
    long result = 0;
    for(BufferedStream stream: streams.values()) {
      result += stream.getBufferSize();
    }
    result += treeWriter.estimateMemory();
    return result;
  }

  @Override
  public synchronized void addUserMetadata(String name, ByteBuffer value) {
    userMetadata.put(name, ByteString.copyFrom(value));
  }

  @Override
  public void addRow(Object row) throws IOException {
    synchronized (this) {
      treeWriter.write(row);
      rowsInStripe += 1;
      if (buildIndex) {
        rowsInIndex += 1;

        if (rowsInIndex >= rowIndexStride) {
          createRowIndexEntry();
        }
      }
    }
    memoryManager.addedRow();
  }

  @Override
  public long getRowRawDataSize() {
    return treeWriter.getRowRawDataSize();
  }

  @Override
  public void close() throws IOException {
    close(null);
  }

  void close(ColumnStatisticsImpl[] columnStats) throws IOException {
    // remove us from the memory manager so that we don't get any callbacks
    memoryManager.removeWriter(path);
    // actually close the file
    synchronized (this) {
      flushStripe();
      int footerLength = writeFooter(rawWriter.getPos(), columnStats);
      rawWriter.writeByte(writePostScript(footerLength));
      rawWriter.close();
    }
  }
}
