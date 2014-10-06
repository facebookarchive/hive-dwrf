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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import com.facebook.hive.orc.compression.CompressionCodec;
import com.facebook.hive.orc.compression.CompressionKind;
import com.facebook.hive.orc.compression.ZlibCodec;
import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.junit.Test;

import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;
import com.facebook.hive.orc.WriterImpl.RowIndexPositionRecorder;

public class TestInStream {

  static class OutputCollector implements OutStream.OutputReceiver {
    DynamicByteArray buffer = new DynamicByteArray();

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      this.buffer.add(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }
  }

  @Test
  public void testUncompressed() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", 100, null, collect);
    RowIndex.Builder rowIndex = OrcProto.RowIndex.newBuilder();
    RowIndexEntry.Builder rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
    WriterImpl.RowIndexPositionRecorder rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
    for(int i=0; i < 1024; ++i) {
      out.getPosition(rowIndexPosition);
      rowIndex.addEntry(rowIndexEntry.build());
      rowIndexEntry.clear();
      out.write(i);
    }
    out.flush();
    assertEquals(1024, collect.buffer.size());
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, collect.buffer.get(i));
    }
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", inBuf, null, 100);
    assertEquals("uncompressed stream test base: 0 offset: 0 limit: 1024",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    in.loadIndeces(rowIndex.build().getEntryList(), 0);
    for(int i=1023; i >= 0; --i) {
      in.seek(i);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCompressed() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 300, codec, collect);
    RowIndex.Builder rowIndex = OrcProto.RowIndex.newBuilder();
    RowIndexEntry.Builder rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
    WriterImpl.RowIndexPositionRecorder rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
    for(int i=0; i < 1024; ++i) {
      out.getPosition(rowIndexPosition);
      rowIndex.addEntry(rowIndexEntry.build());
      rowIndexEntry.clear();
      out.write(i);
    }
    out.flush();
    assertEquals("test", out.toString());
    assertEquals(961, collect.buffer.size());
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", inBuf, codec, 300);
    assertEquals("compressed stream test base: 0 limit: 961 current stride: 1 compressed offset: 0",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    in.loadIndeces(rowIndex.build().getEntryList(), 0);
    for(int i=1023; i >= 0; --i) {
      in.seek(i);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCorruptStream() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    OutStream out = new OutStream("test", 500, codec, collect);
    for(int i=0; i < 1024; ++i) {
      out.write(i);
    }
    out.flush();

    // now try to read the stream with a buffer that is too small
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", inBuf, codec, 100);
    byte[] contents = new byte[1024];
    try {
      in.read(contents);
      fail();
    } catch(IllegalArgumentException iae) {
      // EXPECTED
    }

    // make a corrupted header
    inBuf.clear();
    inBuf.put((byte) 32);
    inBuf.put((byte) 0);
    inBuf.flip();
    in = InStream.create("test2", inBuf, codec, 300);
    try {
      in.read();
      fail();
    } catch (IllegalStateException ise) {
      // EXPECTED
    }
  }

  /**
   *
   * TestFSDataInputStream.
   *
   * Implementation of FSDataInputStream for testing, seek asserts that it is being called with the
   * value passed to the constructor.
   */
  private static class TestFSDataInputStream extends FSDataInputStream {

    private final long expectedSeek;

    public TestFSDataInputStream(InputStream stream, long expectedSeek) throws IOException {
      super(stream);
      this.expectedSeek = expectedSeek;
    }

    @Override
    public synchronized void seek(long desired) throws IOException {
      Assert.assertEquals("Seeking by an unexpected amount", expectedSeek, desired);
    }
  }

  /**
   *
   * TestInputStream.
   *
   * Implementation of InputStream, Seekable, PositionedReadable suitable for passing into a test
   * implementation of FSDataInputStream.  All methods are overridden using the default
   * implementation except read() which invariably returns 1.
   */
  private static class TestInputStream extends InputStream implements Seekable, PositionedReadable {

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void seek(long arg0) throws IOException {
    }

    @Override
    public boolean seekToNewSource(long arg0) throws IOException {
      return false;
    }

    @Override
    public int read(long arg0, byte[] arg1, int arg2, int arg3) throws IOException {
      return 0;
    }

    @Override
    public void readFully(long arg0, byte[] arg1) throws IOException {
    }

    @Override
    public void readFully(long arg0, byte[] arg1, int arg2, int arg3) throws IOException {
    }

    @Override
    public int read() throws IOException {
      return 1;
    }

    public List<ByteBuffer> readFullyScatterGather(long arg0, int arg1) throws IOException {
      return null;
    }

  }

  /**
   * Tests reading from a file so big that the offset to read from exceeds the size of an integer
   * @throws IOException
   */
  @Test
  public void testBigFile() throws IOException {
    InputStream inputStream = new TestInputStream();
    // Assert that seek is called with a value outside the range of ints
    TestFSDataInputStream stream = new TestFSDataInputStream(inputStream,
        (long) Integer.MAX_VALUE + 1);
    // Initialize an InStream with an offset outside the range of ints, the values of the stream
    // length and buffer size (32899 and 32896 respectively) are to accomodate the fact that
    // TestInputStream returns 1 for all calls to read()
    InStream in = InStream.create("testStram", stream, (long) Integer.MAX_VALUE + 1,
        32899, WriterImpl.createCodec(CompressionKind.ZLIB), 32896);
    in.read();
  }
}
