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

import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.junit.Test;

import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;
import com.facebook.hive.orc.WriterImpl.RowIndexPositionRecorder;

public class TestBitFieldReader {

  public void runSeekTest(CompressionCodec codec) throws Exception {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    final int COUNT = 16384;
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", 500, codec, collect), 1);
    RowIndex.Builder rowIndex = OrcProto.RowIndex.newBuilder();
    RowIndexEntry.Builder rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
    WriterImpl.RowIndexPositionRecorder rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
    for(int i=0; i < COUNT; ++i) {
      out.getPosition(rowIndexPosition);
      rowIndex.addEntry(rowIndexEntry.build());
      rowIndexEntry.clear();
      // test runs, non-runs
      if (i < COUNT / 2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create("test", inBuf, codec, 500));
    for(int i=0; i < COUNT; ++i) {
      int x = in.next();
      if (i < COUNT / 2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
    }
    in.loadIndeces(rowIndex.build().getEntryList(), 0);
    for(int i=COUNT-1; i >= 0; --i) {
      in.seek(i);
      int x = in.next();
      if (i < COUNT / 2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
    }
  }

  @Test
  public void testUncompressedSeek() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    runSeekTest(null);
  }

  @Test
  public void testCompressedSeek() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    runSeekTest(new ZlibCodec());
  }

  @Test
  public void testSkips() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    BitFieldWriter out = new BitFieldWriter(
        new OutStream("test", 100, null, collect), 1);
    final int COUNT = 16384;
    for(int i=0; i < COUNT; ++i) {
      if (i < COUNT/2) {
        out.write(i & 1);
      } else {
        out.write((i/3) & 1);
      }
    }
    out.flush();
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    BitFieldReader in = new BitFieldReader(InStream.create
        ("test", inBuf, null, 100));
    for(int i=0; i < COUNT; i += 5) {
      int x = (int) in.next();
      if (i < COUNT/2) {
        assertEquals(i & 1, x);
      } else {
        assertEquals((i/3) & 1, x);
      }
      if (i < COUNT - 5) {
        in.skip(4);
      }
      in.skip(0);
    }
  }
}
