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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.WritableComparable;

/**
 * Contains factory methods to read or write ORC files.
 */
public final class OrcFile {

  public static final String MAGIC = "ORC";
  public static final String COMPRESSION = "orc.compress";
  public static final String COMPRESSION_BLOCK_SIZE = "orc.compress.size";
  public static final String STRIPE_SIZE = "orc.stripe.size";
  public static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  public static final String ENABLE_INDEXES = "orc.create.index";

  // unused
  private OrcFile() {}

  public static class KeyWrapper implements WritableComparable<KeyWrapper> {
    public StripeInformation key;
    public Path inputPath;
    public ObjectInspector objectInspector;
    public CompressionKind compression;
    public int compressionSize;
    public int rowIndexStride;
    public ColumnStatistics[] columnStats;
    public Map<String, ByteBuffer> userMetadata;

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new RuntimeException("Not supported.");

    }
    @Override
    public void write(DataOutput out) throws IOException {
      throw new RuntimeException("Not supported.");

    }
    @Override
    public int compareTo(KeyWrapper o) {
      throw new RuntimeException("Not supported.");
    }
  }

  public static class ValueWrapper implements WritableComparable<ValueWrapper> {
    public byte[] value;

    @Override
    public void readFields(DataInput in) throws IOException {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public void write(DataOutput out) throws IOException {
      throw new RuntimeException("Not supported.");
    }

    @Override
    public int compareTo(ValueWrapper o) {
      throw new RuntimeException("Not supported.");
    }
  }

  /**
   * Create an ORC file reader.
   * @param fs file system
   * @param path file name to read from
   * @return a new ORC file reader.
   * @throws IOException
   */
  public static Reader createReader(FileSystem fs, Path path
                                    ) throws IOException {
    return new ReaderImpl(fs, path);
  }

  /**
   * Create an ORC file streamFactory.
   * @param fs file system
   * @param path filename to write to
   * @param inspector the ObjectInspector that inspects the rows
   * @param stripeSize the number of bytes in a stripe
   * @param compress how to compress the file
   * @param bufferSize the number of bytes to compress at once
   * @param rowIndexStride the number of rows between row index entries or
   *                       0 to suppress all indexes
   * @return a new ORC file streamFactory
   * @throws IOException
   */
  public static Writer createWriter(FileSystem fs,
                                    Path path,
                                    Configuration conf,
                                    ObjectInspector inspector,
                                    long stripeSize,
                                    CompressionKind compress,
                                    int bufferSize,
                                    int rowIndexStride) throws IOException {
    return new WriterImpl(fs, path, conf, inspector, stripeSize, compress,
      bufferSize, rowIndexStride, getMemoryManager(conf));
  }

  private static MemoryManager memoryManager = null;

  private static synchronized MemoryManager getMemoryManager(
      Configuration conf) {

    if (memoryManager == null) {
      memoryManager = new MemoryManager(conf);
    }
    return memoryManager;
  }
}
