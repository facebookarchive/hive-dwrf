package com.facebook.hive.orc;

import com.facebook.hive.orc.compression.CompressionKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;

public class WriterImplWithForceFlush extends WriterImpl {
  public WriterImplWithForceFlush(FileSystem fs, Path path, Configuration conf,
      ObjectInspector inspector, long stripeSize, CompressionKind compress, int bufferSize,
      int rowIndexStride, MemoryManager memoryManager) throws IOException {
    super(fs, path, conf, inspector, stripeSize, compress, bufferSize, rowIndexStride,
        memoryManager);
  }

  public void forceFlushStripe() throws IOException {
    flushStripe();
  }
}
