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

import java.nio.ByteBuffer;

import com.facebook.hive.orc.compression.CompressionCodec;
import com.facebook.hive.orc.compression.SnappyCodec;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.junit.Test;

import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;
import com.facebook.hive.orc.WriterImpl.RowIndexPositionRecorder;

public class TestRunLengthByteReader {

  @Test
  public void testUncompressedSeek() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();

    ReaderWriterProfiler.setProfilerOptions(null);
    RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test", 100,
        null, collect, new MemoryEstimate()));
    RowIndex.Builder rowIndex = OrcProto.RowIndex.newBuilder();
    RowIndexEntry.Builder rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
    WriterImpl.RowIndexPositionRecorder rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
    for(int i=0; i < 2048; ++i) {
      out.getPosition(rowIndexPosition);
      rowIndex.addEntry(rowIndexEntry.build());
      rowIndexEntry.clear();
      if (i < 1024) {
        out.write((byte) (i/4));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    ReaderWriterProfiler.setProfilerOptions(null);
    RunLengthByteReader in = new RunLengthByteReader(InStream.create("test",
        inBuf, null, 100));
    for(int i=0; i < 2048; ++i) {
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
    in.loadIndeces(rowIndex.build().getEntryList(), 0);
    for(int i=2047; i >= 0; --i) {
      in.seek(i);
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
  }

  @Test
  public void testCompressedSeek() throws Exception {
    CompressionCodec codec = new SnappyCodec();
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();

    ReaderWriterProfiler.setProfilerOptions(null);
    RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test", 500,
        codec, collect, new MemoryEstimate()));
    RowIndex.Builder rowIndex = OrcProto.RowIndex.newBuilder();
    RowIndexEntry.Builder rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
    WriterImpl.RowIndexPositionRecorder rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
    for(int i=0; i < 2048; ++i) {
      out.getPosition(rowIndexPosition);
      rowIndex.addEntry(rowIndexEntry.build());
      rowIndexEntry.clear();
      if (i < 1024) {
        out.write((byte) (i/4));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    ReaderWriterProfiler.setProfilerOptions(null);
    RunLengthByteReader in = new RunLengthByteReader(InStream.create("test",
        inBuf, codec, 500));
    for(int i=0; i < 2048; ++i) {
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
    in.loadIndeces(rowIndex.build().getEntryList(), 0);
    for(int i=2047; i >= 0; --i) {
      in.seek(i);
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/4) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
    }
  }

  @Test
  public void testSkips() throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    ReaderWriterProfiler.setProfilerOptions(null);
    RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test", 100,
        null, collect, new MemoryEstimate()));
    for(int i=0; i < 2048; ++i) {
      if (i < 1024) {
        out.write((byte) (i/16));
      } else {
        out.write((byte) i);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    ReaderWriterProfiler.setProfilerOptions(null);
    RunLengthByteReader in = new RunLengthByteReader(InStream.create("test",
        inBuf, null, 100));
    for(int i=0; i < 2048; i += 10) {
      int x = in.next() & 0xff;
      if (i < 1024) {
        assertEquals((i/16) & 0xff, x);
      } else {
        assertEquals(i & 0xff, x);
      }
      if (i < 2038) {
        in.skip(9);
      }
      in.skip(0);
    }
  }
}
