package com.facebook.hive.orc;

import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Class for testing RecordReaderImpl.
 */
public class TestRecordReaderImpl {
  private static final String TMP_PATH_NAME = "dwrf_test_folder";
  private File stagingDir;
  static {
    // Setting profiler options to null is required for dwrf reading.
    ReaderWriterProfiler.setProfilerOptions(null);
  }

  @Before
  public void setUp() throws IOException {
    java.nio.file.Path tmpPath = Files.createTempDirectory(TMP_PATH_NAME);
    this.stagingDir = new File(tmpPath.toString());
    this.stagingDir.deleteOnExit();
  }

  @Test
  public void testPartialReadingWithSeeks() throws IOException {
    // First we create a file of a small size and having two stripes.
    final String fileName = "file_with_long_col";
    final JobConf jobConf = new JobConf();
    jobConf.setLong(OrcConf.ConfVars.HIVE_ORC_STRIPE_SIZE.varname, 1024L);
    final Path filePath = new Path(this.stagingDir.toString(), fileName);
    final RecordWriter hiveRecordWriter = (RecordWriter) (new OrcOutputFormat().getHiveRecordWriter(
        jobConf, filePath, null, true, new Properties(), null));

    final OrcSerde orcSerde = new OrcSerde();
    ObjectInspector objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(
        Long.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

    for (int i = 0; i < 1000; i++) {
      Object obj = orcSerde.serialize(75L, objectInspector);
      hiveRecordWriter.write(NullWritable.get(), obj);
    }
    hiveRecordWriter.close(null);

    // Get all the stripes in the file written.
    final Configuration configuration = new Configuration();
    final FileSystem fileSystem = filePath.getFileSystem(configuration);
    final ReaderImpl readerImpl = new ReaderImpl(fileSystem, filePath, configuration);
    final ArrayList<StripeInformation> stripes = Lists.newArrayList(readerImpl.getStripes());
    assertTrue("Number of stripes produced should be >= 2", stripes.size() >= 2);

    // Read the file back and read with ReaderImpl over only the 2nd stripe.
    final boolean[] toRead = {true};
    final RecordReader recordReader = new ReaderImpl(fileSystem, filePath, configuration).rows(
        stripes.get(1).getOffset(), stripes.get(1).getDataLength(), toRead);
    OrcLazyRow row = null;
    while (recordReader.hasNext()) {
      row = (OrcLazyRow) recordReader.next(row);
    }

    // Seek to the beginning of the 2nd stripe and ensure that seeking works fine.
    recordReader.seekToRow(stripes.get(0).getNumberOfRows());
    assertEquals(recordReader.getRowNumber(), stripes.get(0).getNumberOfRows());
  }
}
