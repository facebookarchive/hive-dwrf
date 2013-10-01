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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.SerDeStatsSupplier;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import com.facebook.hive.orc.OrcSerde.OrcSerdeRow;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * A Hive OutputFormat for ORC files.
 */
public class OrcOutputFormat extends FileOutputFormat<NullWritable, OrcSerdeRow>
        implements HiveOutputFormat<NullWritable, OrcSerdeRow> {

    private final MemoryManagerProvider memoryManagerProvider;

    public OrcOutputFormat(MemoryManager memoryManager) {
        memoryManagerProvider = MemoryManagerProviders.fromInstance(memoryManager);
    }

    public OrcOutputFormat() {
        memoryManagerProvider = MemoryManagerProviders.staticSingleton();
    }

    private static class OrcRecordWriter
            implements RecordWriter<NullWritable, OrcSerdeRow>,
            SerDeStatsSupplier,
            FileSinkOperator.RecordWriter {
        private Writer writer = null;
        private final FileSystem fs;
        private final Path path;
        private final Configuration conf;
        private final long stripeSize;
        private final int compressionSize;
        private final CompressionKind compress;
        private final int rowIndexStride;
        private final MemoryManager memoryManager;
        private final SerDeStats stats;

        OrcRecordWriter(FileSystem fs, Path path, Configuration conf,
                long stripeSize, String compress,
                int compressionSize, int rowIndexStride, MemoryManager memoryManager) {
            this.fs = fs;
            this.path = path;
            this.conf = conf;
            this.stripeSize = stripeSize;
            this.compress = CompressionKind.valueOf(compress);
            this.compressionSize = compressionSize;
            this.rowIndexStride = rowIndexStride;
            this.memoryManager = memoryManager;
            this.stats = new SerDeStats();
        }

        @Override
        public void write(NullWritable nullWritable,
                OrcSerdeRow row) throws IOException {
            if (writer == null) {
                writer = OrcFile.createWriter(fs, path, this.conf, row.getInspector(),
                        stripeSize, compress, compressionSize, rowIndexStride, memoryManager);
            }
            writer.addRow(row.getRow());
        }

        @Override
        public void write(Writable row) throws IOException {
            OrcSerdeRow serdeRow = (OrcSerdeRow) row;
            if (writer == null) {
                writer = OrcFile.createWriter(fs, path, this.conf,
                        serdeRow.getInspector(), stripeSize, compress, compressionSize,
                        rowIndexStride, memoryManager);
            }
            writer.addRow(serdeRow.getRow());
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            close(true);
        }

        @Override
        public void close(boolean b) throws IOException {
            // if we haven't written any rows, we need to create a file with a
            // generic schema.
            if (writer == null) {
                // a row with no columns
                ObjectInspector inspector = ObjectInspectorFactory.
                        getStandardStructObjectInspector(new ArrayList<String>(),
                                new ArrayList<ObjectInspector>());
                writer = OrcFile.createWriter(fs, path, this.conf, inspector,
                        stripeSize, compress, compressionSize, rowIndexStride, memoryManager);
            }
            writer.close();
        }

        @Override
        public SerDeStats getStats() {
            stats.setRawDataSize(writer.getRowRawDataSize());
            return stats;
        }
    }

    @Override
    public RecordWriter<NullWritable, OrcSerdeRow>
    getRecordWriter(FileSystem fileSystem, JobConf conf, String name,
            Progressable reporter) throws IOException {
        return new OrcRecordWriter(fileSystem,  new Path(name), conf,
                OrcConfVars.getStripeSize(conf),
                OrcConfVars.getCompression(conf),
                OrcConfVars.getCompressSize(conf),
                OrcConfVars.getRowIndexStride(conf),
                memoryManagerProvider.get(conf));
    }

    @Override
    public FileSinkOperator.RecordWriter
    getHiveRecordWriter(JobConf conf,
            Path path,
            Class<? extends Writable> valueClass,
            boolean isCompressed,
            Properties tableProperties,
            Progressable reporter) throws IOException {
        String stripeSizeStr = tableProperties.getProperty(OrcFile.STRIPE_SIZE);
        long stripeSize;
        if (stripeSizeStr != null) {
            stripeSize = Long.valueOf(stripeSizeStr);
        } else {
            stripeSize = OrcConfVars.getStripeSize(conf);
        }

        String compression = tableProperties.getProperty(OrcFile.COMPRESSION);
        if (compression == null) {
            compression = OrcConfVars.getCompression(conf);
        }

        String compressionSizeStr = tableProperties.getProperty(OrcFile.COMPRESSION_BLOCK_SIZE);
        int compressionSize;
        if (compressionSizeStr != null) {
            compressionSize = Integer.valueOf(compressionSizeStr);
        } else {
            compressionSize = OrcConfVars.getCompressSize(conf);
        }

        String rowIndexStrideStr = tableProperties.getProperty(OrcFile.ROW_INDEX_STRIDE);
        int rowIndexStride;
        if (rowIndexStrideStr != null) {
            rowIndexStride = Integer.valueOf(rowIndexStrideStr);
        } else {
            rowIndexStride = OrcConfVars.getRowIndexStride(conf);
        }

        String enableIndexesStr = tableProperties.getProperty(OrcFile.ENABLE_INDEXES);
        boolean enableIndexes;
        if (enableIndexesStr != null) {
            enableIndexes = Boolean.valueOf(enableIndexesStr);
        } else {
            enableIndexes = OrcConfVars.getCreateIndex(conf);
        }

        if (!enableIndexes) {
            rowIndexStride = 0;
        }

        return new OrcRecordWriter(path.getFileSystem(conf), path, conf,
                stripeSize, compression, compressionSize, rowIndexStride,
                memoryManagerProvider.get(conf));
    }
}
