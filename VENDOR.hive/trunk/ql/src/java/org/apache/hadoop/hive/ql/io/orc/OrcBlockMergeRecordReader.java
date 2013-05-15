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
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.KeyWrapper;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ValueWrapper;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("deprecation")
public class OrcBlockMergeRecordReader implements
    RecordReader<KeyWrapper, ValueWrapper> {

  private final Reader in;
  private final StripeReader stripeIn;
  private boolean more = true;
  private final Path path;
  protected Configuration conf;

  public OrcBlockMergeRecordReader(Configuration conf, FileSplit split)
      throws IOException {
    path = split.getPath();
    FileSystem fs = path.getFileSystem(conf);
    in = new ReaderImpl(fs, path);
    stripeIn = in.stripes(split.getStart(), split.getLength());
    this.conf = conf;

    more = stripeIn.hasNext();
  }

  public Class<?> getKeyClass() {
    return KeyWrapper.class;
  }

  public Class<?> getValueClass() {
    return ValueWrapper.class;
  }

  @Override
  public KeyWrapper createKey() {
    return new KeyWrapper();
  }

  @Override
  public ValueWrapper createValue() {
    return new ValueWrapper();
  }

  @Override
  public boolean next(KeyWrapper keyWrapper, ValueWrapper valueWrapper)
      throws IOException {
    if (more) {
      more = stripeIn.nextStripe(keyWrapper, valueWrapper);
      keyWrapper.inputPath = path;
      keyWrapper.objectInspector = in.getObjectInspector();
      keyWrapper.compression = in.getCompression();
      keyWrapper.compressionSize = in.getCompressionSize();
      keyWrapper.rowIndexStride = in.getRowIndexStride();
      keyWrapper.columnStats = in.getStatistics();
      keyWrapper.userMetadata = new HashMap<String, ByteBuffer>();
      for (String key : in.getMetadataKeys()) {
        keyWrapper.userMetadata.put(key, in.getMetadataValue(key));
      }
      return true;
    }
    return false;
  }

  /**
   * Return the progress within the input split.
   *
   * @return 0.0 to 1.0 of the input byte range
   */
  @Override
  public float getProgress() throws IOException {
    return stripeIn.getProgress();
  }

  @Override
  public long getPos() throws IOException {
    return stripeIn.getPosition();
  }

  @Override
  public void close() throws IOException {
    stripeIn.close();
  }

}
