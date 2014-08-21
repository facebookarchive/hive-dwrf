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

import com.facebook.hive.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.facebook.hive.orc.compression.CompressionCodec;
import org.apache.hadoop.fs.FSDataInputStream;

import com.facebook.hive.orc.OrcProto.RowIndexEntry;

public abstract class InStream extends InputStream {

  public static final int END_OF_BUFFER = -1;
  private final boolean useVInts;

  private static class UncompressedStream extends InStream {
    private final String name;
    // The file this stream is to read data from
    private final FSDataInputStream file;
    private byte[] array;
    private int offset;
    private final long base;
    private final int limit;
    // The number of bytes into the stream we are at each index
    private int[] indeces;

    public UncompressedStream(String name, FSDataInputStream file, long streamOffset,
        int streamLength, boolean useVInts) {
      super(useVInts);

      this.name = name;
      this.array = null;
      this.file = file;
      this.base = streamOffset;
      this.limit = streamLength;
      this.offset = 0;
    }

    public UncompressedStream(String name, ByteBuffer input, boolean useVInts) {
      super(useVInts);

      this.name = name;
      this.array = input.array();
      this.base = input.arrayOffset() + input.position();
      this.offset = (int) base;
      this.limit = input.arrayOffset() + input.limit();
      this.file = null;
    }

    @Override
    public int read() throws IOException {
      if (offset == limit) {
        return END_OF_BUFFER;
      }
      if (array == null) {
        array = new byte[limit];
        file.read(base, array, 0, limit);
      }
      return 0xff & array[offset++];
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (this.offset == limit) {
        return END_OF_BUFFER;
      }
      if (array == null) {
        array = new byte[limit];
        file.read(base, array, 0, limit);
      }
      int actualLength = Math.min(length, limit - this.offset);
      System.arraycopy(array, this.offset, data, offset, actualLength);
      this.offset += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      return limit - offset;
    }

    @Override
    public void close() {
      array = null;
      offset = 0;
    }

    @Override
    public void seek(int index) throws IOException {
      offset = (int) base + indeces[index];
    }

    @Override
    public String toString() {
      return "uncompressed stream " + name + " base: " + base +
         " offset: " + offset + " limit: " + limit;
    }

    @Override
    public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
      indeces = new int[rowIndexEntries.size()];
      int i = 0;
      for (RowIndexEntry rowIndexEntry : rowIndexEntries) {
        indeces[i] = (int) rowIndexEntry.getPositions(startIndex);
        i++;
      }

      return startIndex + 1;
    }
  }

  private static class CompressedStream extends InStream {
    private final String name;
    private byte[] array;
    private final int bufferSize;
    private ByteBuffer uncompressed = null;
    private final CompressionCodec codec;
    private final FSDataInputStream file;
    private final long base;
    private final int limit;
    private boolean isUncompressedOriginal;
    // For each index, the start position of the compression block
    private int[] compressedIndeces;
    // For each index, a secondary index into strideStarts
    private int[] compressedStrides;
    // For each index, in the current compression block, how many bytes of uncompressed data
    // should be skipped
    private int[] uncompressedIndeces;
    // The start positions of chunks, I'm defining a chunk as a set of index strides where the
    // number of distinct values for the compressed index is equal to readStrides.  This means
    // A chunk may contain more than readStrides index strides, if two or more index strides
    // fit in a single compression block.
    private long[] chunkStarts;
    // The number of bytes we currently have read from the file and have available in memory
    private int chunkLength;
    // How much of the compressed data in memory has been read
    private int compressedOffset;
    // The previous value of compressedOffset
    private int previousOffset = -1;
    // The current chunk that is being read
    private int currentChunk;
    // The total number of chunks
    private int numChunks;
    // The number of strides to read in from HDFS at a time
    private final int readStrides;

    public CompressedStream(String name, FSDataInputStream file, long streamOffset,
        int streamLength, CompressionCodec codec, int bufferSize, boolean useVInts,
        int readStrides) {
      super(useVInts);

      this.array = null;
      this.name = name;
      this.codec = codec;
      this.bufferSize = bufferSize;
      this.readStrides = readStrides;
      this.file = file;
      // Initialize assuming the stream is one giant stride, if there are multiple strides, these
      // assumptions will be fixed by the call to loadIndeces
      this.base = streamOffset;
      this.limit = streamLength;
      this.currentChunk = 0;
      this.compressedOffset = limit;
      this.chunkLength = limit;
      // If the limit is 0, there is no data, so no strides, otherwise, assume it's just one giant
      // stride.  This will get fixed by loadIndeces if it's wrong.
      this.numChunks = limit == 0 ? 0 : 1;
    }

    public CompressedStream(String name, ByteBuffer input, CompressionCodec codec, int bufferSize,
        boolean useVInts) {
      super(useVInts);

      this.array = input.array();
      this.name = name;
      this.codec = codec;
      this.bufferSize = bufferSize;
      this.base = input.arrayOffset() + input.position();
      this.compressedOffset = (int) base;
      this.limit = input.arrayOffset() + input.limit();
      this.file = null;
      this.readStrides = -1;
      this.currentChunk = 1;
      this.numChunks = 1;
      this.chunkLength = limit;
    }

    @Override
    public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
      int numIndeces = rowIndexEntries.size();
      // CompressedStreams have two values per index, the start position of the compressed chunk
      // it should currently be reading, and how many bytes of the uncompressed version of that
      // chunk we've read
      compressedStrides = new int[numIndeces];
      compressedIndeces = new int[numIndeces];
      uncompressedIndeces = new int[numIndeces];
      chunkStarts = new long[numIndeces + 1];
      int maxLength = 0;
      int length = 0;
      int i = 0;
      numChunks = 1;
      chunkStarts[0] = base;
      int compressedIndex;
      RowIndexEntry rowIndexEntry;
      int distinctStrides = 0;
      int previousStrideStart = 0;
      for (i = 0; i < rowIndexEntries.size(); i++) {
        rowIndexEntry = rowIndexEntries.get(i);
        compressedIndex = (int) rowIndexEntry.getPositions(startIndex);
        // chunkStarts contains unique values of the compressedIndex
        // note that base + compressedIndex = the file offset, and cunkStarts contains file
        // offsets
        if (compressedIndex != previousStrideStart) {
          previousStrideStart = compressedIndex;
          distinctStrides++;
          if (distinctStrides == readStrides) {
            // If the comprssedIndex is new (should be monotonically increasing)
            // convert it to a file offset
            chunkStarts[numChunks] = base + compressedIndex;

            // the length of the previous chunk
            length = (int) (chunkStarts[numChunks] - chunkStarts[numChunks - 1]);
            // update max length if necessary
            maxLength = maxLength < length ? length : maxLength;
            numChunks++;
            distinctStrides = 0;
          }
        }
        compressedStrides[i] = numChunks - 1;
        compressedIndeces[i] = compressedIndex;
        uncompressedIndeces[i] = (int) rowIndexEntry.getPositions(startIndex + 1);
      }

      // The final value in chunkStarts is the offset of the end of the stream data
      chunkStarts[numChunks] = base + limit;
      // Compute the length of the final stride
      length = (int) (chunkStarts[numChunks] - chunkStarts[numChunks - 1]);
      // Update max length if necessary
      maxLength = maxLength < length ? length : maxLength;

      // Initialize array to an array that can contain the largest stride
      if (array == null) {
        this.array = new byte[maxLength];
      }

      // Return a value of start index that will skip the 2 values read in this method
      return startIndex + 2;
    }

    private void readData() throws IOException {
      if (file == null) {
        // If file is null, this InStream was initialized using a ByteBuffer, so there's no need
        // to read anything from disk
        return;
      }

      long fileOffset = base;
      chunkLength = limit;
      if (chunkStarts != null) {
        // If chunkStarts is not null, loadIndeces was called, so don't treat it as a single
        // giant stride
        fileOffset = chunkStarts[currentChunk];
        chunkLength = (int) (chunkStarts[currentChunk + 1] - chunkStarts[currentChunk]);
      } else if (array == null) {
        // Otherwise treat it as a single giant stride, initialize the array if necessary
        array = new byte[chunkLength];
      }

      InStream.read(file, fileOffset, array, 0, chunkLength);

      // Should read the next stride when this if block is entered again
      currentChunk++;
      // No compressed data has been read yet
      compressedOffset = 0;
    }

    private void readHeader() throws IOException {
      if (compressedOffset >= chunkLength) {
        readData();
      }

      // There should be at least enough bytes read that there is a room for a header
      if (chunkLength - compressedOffset <= OutStream.HEADER_SIZE) {
        throw new IllegalStateException("Can't read header");
      }

      previousOffset = compressedOffset;
      int chunkLength = ((0xff & array[compressedOffset + 2]) << 15) |
        ((0xff & array[compressedOffset + 1]) << 7) | ((0xff & array[compressedOffset]) >> 1);
      if (chunkLength > bufferSize) {
        throw new IllegalArgumentException("Buffer size too small. size = " +
            bufferSize + " needed = " + chunkLength);
      }
      boolean isOriginal = (array[compressedOffset] & 0x01) == 1;
      compressedOffset += OutStream.HEADER_SIZE;
      if (isOriginal) {
        isUncompressedOriginal = true;
        uncompressed = ByteBuffer.wrap(array, compressedOffset, chunkLength);
      } else {
        if (isUncompressedOriginal) {
          uncompressed = ByteBuffer.allocate(bufferSize);
          isUncompressedOriginal = false;
        } else if (uncompressed == null) {
          uncompressed = ByteBuffer.allocate(bufferSize);
        } else {
          uncompressed.clear();
        }
        codec.decompress(ByteBuffer.wrap(array, compressedOffset, chunkLength),
          uncompressed);
      }
      compressedOffset += chunkLength;
    }

    @Override
    public int read() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        // If all chunks have been read, and all data from this chunk has been read, there's no
        // data left to read
        if (currentChunk >= numChunks && compressedOffset >= chunkLength) {
          return END_OF_BUFFER;
        }
        readHeader();
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        // If all chunks have been read, and all data from this chunk has been read, there's no
        // data left to read
        if (currentChunk >= numChunks && compressedOffset >= chunkLength) {
          return END_OF_BUFFER;
        }
        readHeader();
      }
      int actualLength = Math.min(length, uncompressed.remaining());
      System.arraycopy(uncompressed.array(),
        uncompressed.arrayOffset() + uncompressed.position(), data,
        offset, actualLength);
      uncompressed.position(uncompressed.position() + actualLength);
      return actualLength;
    }

    @Override
    public int available() throws IOException {
      if (uncompressed == null || uncompressed.remaining() == 0) {
        // If all chunks have been read, and all data from this chunk has been read, there's no
        // data left to read
        if (currentChunk >= numChunks && compressedOffset >= chunkLength) {
          return 0;
        }
        readHeader();
      }
      return uncompressed.remaining();
    }

    @Override
    public void close() {
      array = null;
      uncompressed = null;
      // Make sure if anyone tries to read, it returns nothing.
      compressedOffset = chunkLength;
      currentChunk = numChunks;
    }

    @Override
    public void seek(int index) throws IOException {
      int uncompBytes = uncompressedIndeces[index];
      // If file is null the compressed offset should be relative to the start of the ByteBuffer
      // that was used to initialize this InStream, otherwise, it is relative to where this data
      // starts in the file.
      int newCompressedOffset = file == null ? (int) base + compressedIndeces[index] :
        (int) (compressedIndeces[index] - (chunkStarts[compressedStrides[index]] - base));
      if (uncompBytes != 0 || uncompressed != null) {

        boolean dataRead = false;
        // If uncompressed has been initialized and the offset we're seeking to is the same as the offset
        // we're reading, no need to re-decompress
        if (currentChunk - 1 != compressedStrides[index]) {
          currentChunk = compressedStrides[index];
          // currentStride has been updated, so force the data to be reread from disk
          readData();
          dataRead = true;
        }

        if (dataRead || previousOffset != newCompressedOffset) {
          compressedOffset = newCompressedOffset;
          readHeader();
        }
        // If the data was not compressed the starting position is the position of the data in the stream
        // Otherwise it's 0
        uncompressed.position((isUncompressedOriginal ? newCompressedOffset + OutStream.HEADER_SIZE : 0) + uncompBytes);
      } else {
        // Otherwise uncompressed is null and for this index, no bytes of uncompressed data need
        // to be skipped, so it is sufficient to update currentStride, if read is called, it will
        // read the appropriate stride from disk
        if (file != null) {
          // if file is not null we need to read compressed data for the specified index from the
          // file, otherwise we are using a ByteBuffer and there's no need to read any data
          currentChunk = compressedStrides[index];
          readData();
        }
        compressedOffset = newCompressedOffset;
      }
    }

    @Override
    public String toString() {
      return "compressed stream " + name + " base: " + base +
          " limit: " + limit + " current stride: " + currentChunk +
          " compressed offset: " + compressedOffset +
          (uncompressed == null ? "" :
              " uncompressed: " + uncompressed.position() + " to " +
                  uncompressed.limit());
    }
  }

  protected InStream(boolean useVInts) {
    this.useVInts = useVInts;
  }

  /**
   * Read in any indeces for the stream from the rowIndexEntries, startIndex is the index of the
   * first value to read from each rowIndexEntry.  Should return the updated startIndex for the
   * next stream.
   */
  public abstract int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex);

  public abstract void seek(int index) throws IOException;

  /**
   * This should be used for creating streams to read file metadata, e.g. the footer, not for
   * data in columns.
   */
  public static InStream create(String name, FSDataInputStream file, long streamOffset,
      int streamLength, CompressionCodec codec, int bufferSize) throws IOException {

    return create(name, file, streamOffset, streamLength, codec, bufferSize, true, 1);
  }

  public static InStream create(String name, FSDataInputStream file, long streamOffset,
      int streamLength, CompressionCodec codec, int bufferSize, boolean useVInts, int readStrides)
  throws IOException {
    if (codec == null) {
      return new UncompressedStream(name, file, streamOffset, streamLength, useVInts);
    } else {
      return new CompressedStream(name, file, streamOffset, streamLength, codec, bufferSize, useVInts, readStrides);
    }
  }

  /**
   * This should only be used if the data happens to already be in memory, e.g. for tests
   */
  public static InStream create(String name, ByteBuffer input, CompressionCodec codec,
      int bufferSize) throws IOException {
    return create(name, input, codec, bufferSize, true);
  }

  /**
   * This should only be used if the data happens to already be in memory, e.g. for tests
   */
  public static InStream create(String name, ByteBuffer input, CompressionCodec codec,
      int bufferSize, boolean useVInts) throws IOException {
    if (codec == null) {
      return new UncompressedStream(name, input, useVInts);
    } else {
      return new CompressedStream(name, input, codec, bufferSize, useVInts);
    }
  }

  public boolean useVInts() {
    return useVInts;
  }

  // This is just a utility to wrap how we do reads.  This could also be replace by positional
  // reads at some point.
  public static void read(FSDataInputStream file, long fileOffset, byte[] array, int arrayOffset,
      int length) throws IOException {
    file.seek(fileOffset);
    file.readFully(array, arrayOffset, length);
  }
}
