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

package com.facebook.hive.ql.io.orc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.merge.MergeMapper;
import com.facebook.hive.ql.io.orc.OrcFile.KeyWrapper;
import com.facebook.hive.ql.io.orc.OrcFile.ValueWrapper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.shims.CombineHiveKey;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class OrcMergeMapper extends MergeMapper implements
    Mapper<Object, ValueWrapper, Object, Object> {

  private ObjectInspector objectInspector;
  private CompressionKind compression;
  private int compressionSize;
  private int rowIndexStride;
  private ColumnStatisticsImpl[] columnStats;
  private Map<String, ByteBuffer> userMetadata;

  private WriterImpl outWriter;

  @Override
  public void map(Object k, ValueWrapper value,
      OutputCollector<Object, Object> output, Reporter reporter)
      throws IOException {
    try {

      KeyWrapper key = null;
      if (k instanceof CombineHiveKey) {
        key = (KeyWrapper) ((CombineHiveKey) k).getKey();
      } else {
        key = (KeyWrapper) k;
      }

      checkAndFixTmpPath(key.inputPath);

      if (outWriter == null) {
        objectInspector = key.objectInspector;
        compression = key.compression;
        compressionSize = key.compressionSize;
        rowIndexStride = key.rowIndexStride;
        userMetadata = key.userMetadata;
          HiveConf conf = new HiveConf(OrcMergeMapper.class);
          outWriter = (WriterImpl)OrcFile.createWriter(fs, outPath,
                  conf, objectInspector,
            OrcConfVars.getStripeSize(conf), // not needed
            compression, compressionSize, rowIndexStride);
        for (Map.Entry<String, ByteBuffer> metadataEntry : userMetadata.entrySet()) {
          outWriter.addUserMetadata(metadataEntry.getKey(), metadataEntry.getValue());
        }
      }

      if (columnStats == null) {
        columnStats = new ColumnStatisticsImpl[key.columnStats.length];
        for (int i = 0; i < columnStats.length; i++) {
          columnStats[i] = (ColumnStatisticsImpl) key.columnStats[i];
        }
      } else if (columnStats.length != key.columnStats.length) {
        throw new IOException(
            "OrcMerge failed because the input files have a different number of columns");
      } else {
        for (int i = 0; i < columnStats.length; i++) {
          columnStats[i].merge((ColumnStatisticsImpl) key.columnStats[i]);
        }
      }

      if (objectInspector != key.objectInspector && !objectInspector.equals(objectInspector)) {
        throw new IOException(
            "OrcMerge failed because the input files use different ObjectInspectors.");
      }
      if (compression != key.compression || compressionSize != key.compressionSize) {
        throw new IOException(
            "OrcMerge failed because the input files have different compression settings.");
      }
      if (rowIndexStride != key.rowIndexStride) {
        throw new IOException(
            "OrcMerge failed because the input files have different row index strides.");
      }
      if (!userMetadata.equals(key.userMetadata)) {
        throw new IOException(
            "OrcMerge failed because the input files have different user metadata.");
      }

      outWriter.addStripe(key.key, value.value);
    } catch (Throwable e) {
      this.exception = true;
      close();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // close writer
    if (outWriter == null) {
      return;
    }

    outWriter.close(columnStats);
    outWriter = null;

    if (!exception) {
      FileStatus fss = fs.getFileStatus(outPath);
      LOG.info("renamed path " + outPath + " to " + finalPath
          + " . File size is " + fss.getLen());
      if (!fs.rename(outPath, finalPath)) {
        throw new IOException("Unable to rename output to " + finalPath);
      }
    } else {
      if (!autoDelete) {
        fs.delete(outPath, true);
      }
    }
  }

}
