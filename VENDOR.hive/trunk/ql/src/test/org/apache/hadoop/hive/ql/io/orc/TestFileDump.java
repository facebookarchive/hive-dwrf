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
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestFileDump {

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    fs.mkdirs(workDir);
    testFilePath = new Path("TestFileDump.testDump.orc");
    fs.delete(testFilePath, false);
  }

  static class MyRecord {
    int i;
    long l;
    String s;
    MyRecord(int i, long l, String s) {
      this.i = i;
      this.l = l;
      this.s = s;
    }
  }

  private static void checkOutput(String expected,
                                  String actual) throws Exception {
    BufferedReader eStream =
        new BufferedReader(new FileReader(expected));
    BufferedReader aStream =
        new BufferedReader(new FileReader(actual));
    String line = eStream.readLine();
    while (line != null) {
      assertEquals(line, aStream.readLine());
      line = eStream.readLine();
    }
    assertNull(eStream.readLine());
    assertNull(aStream.readLine());
  }

  @Test
  public void testDump() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        100000, CompressionKind.SNAPPY, 10000, 10000, new MemoryManager(conf));
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    for(int i=0; i < 21000; ++i) {
      int curNum = r1.nextInt(words.length);

      writer.addRow(new MyRecord(curNum, (long)curNum + (long) Integer.MAX_VALUE,
          words[curNum]));
    }
    writer.close();
    PrintStream origOut = System.out;
    URL expectedFileUrl = Resources.getResource("orc-file-dump.out");
    String outputFilename = workDir + File.separator + "orc-file-dump.out";
    FileOutputStream myOut = new FileOutputStream(outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.getName()});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(expectedFileUrl.getPath(), outputFilename);
  }

  private void testDictionary(Configuration conf, String expectedOutputFilename) throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    // Turn off using the approximate entropy heuristic to turn off dictionary encoding
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_ENTROPY_KEY_STRING_SIZE_THRESHOLD.varname, -1);
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        100000, CompressionKind.SNAPPY, 10000, 10000, new MemoryManager(conf));
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    int nextInt = 0;
    int nextNumIdx = 0;
    int numRows = 21000;
    List<Integer> intVals = new ArrayList<Integer>(words.length);
    List<Long> longVals = new ArrayList<Long>(words.length);

    int prevInt = 0;
    long prevLong = 0;
    for (int i=0; i < numRows; i++) {
      intVals.add(i);
      longVals.add((long)i + (long)Integer.MAX_VALUE);

    }
    Collections.shuffle(intVals, r1);
    Collections.shuffle(longVals, r1);

    for(int i=0; i < numRows; ++i) {
      // Write out the same string twice, this guarantees the fraction of rows with
      // distinct strings is 0.5
      if (i % 2 == 0) {
        nextInt = r1.nextInt(words.length);
        nextNumIdx = i;
        // Append the value of i to the word, this guarantees when an index or word is repeated
        // the actual string is unique.
        words[nextInt] += "-" + i;
      }
      writer.addRow(new MyRecord(intVals.get(nextNumIdx), longVals.get(nextNumIdx),
          words[nextInt]));
    }
    writer.close();
    PrintStream origOut = System.out;
    URL expectedFileUrl = Resources.getResource(expectedOutputFilename);
    String outputFilename = workDir + File.separator + expectedOutputFilename;
    FileOutputStream myOut = new FileOutputStream(outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.getName()});
    System.out.flush();
    System.setOut(origOut);

    checkOutput(expectedFileUrl.getPath(), outputFilename);
  }

  //Test that if the number of distinct characters in distinct strings is less than the configured
  // threshold dictionary encoding is turned off.  If dictionary encoding is turned off the length
  // of the dictionary stream for the column will be 0 in the ORC file dump.
  @Test
  public void testEntropyThreshold() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (MyRecord.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Configuration conf = new Configuration();
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_ENTROPY_KEY_STRING_SIZE_THRESHOLD.varname, 1);
    conf.setInt(HiveConf.ConfVars.HIVE_ORC_ENTROPY_STRING_THRESHOLD.varname, 11);
    // Make sure having too few distinct values won't turn off dictionary encoding
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD.varname, 1);

    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        100000, CompressionKind.SNAPPY, 10000, 10000, new MemoryManager(conf));
    Random r1 = new Random(1);
    for(int i=0; i < 21000; ++i) {
      writer.addRow(new MyRecord(r1.nextInt(), r1.nextLong(),
          Integer.toString(r1.nextInt())));
    }
    writer.close();
    PrintStream origOut = System.out;
    URL expectedFileUrl = Resources.getResource("orc-file-dump-entropy-threshold.out");
    String outputFilename = workDir + File.separator + "orc-file-dump-entropy-threshold.out";
    FileOutputStream myOut = new FileOutputStream(outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.getName()});
    System.out.flush();
    System.setOut(origOut);

    checkOutput(expectedFileUrl.getPath(), outputFilename);
  }

  // Test that if the fraction of rows that have distinct strings is greater than the configured
  // threshold dictionary encoding is turned off.  If dictionary encoding is turned off the length
  // of the dictionary stream for the column will be 0 in the ORC file dump.
  @Test
  public void testDictionaryThreshold() throws Exception {
    Configuration conf = new Configuration();
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD.varname, 0.49f);
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD.varname, 0.49f);
    testDictionary(conf, "orc-file-dump-dictionary-threshold.out");
  }

  @Test
  public void testUnsortedDictionary() throws Exception {
    Configuration conf = new Configuration();
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD.varname, 0.49f);
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD.varname, 0.49f);
    conf.setBoolean(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS.varname, false);
    testDictionary(conf, "orc-file-dump-dictionary-threshold-unsorted.out");

  }


  @Test
  public void testUnsortedDictionary2() throws Exception {
    Configuration conf = new Configuration();
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_STRING_KEY_SIZE_THRESHOLD.varname, 0.51f);
    conf.setFloat(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD.varname, 0.51f);
    conf.setBoolean(HiveConf.ConfVars.HIVE_ORC_DICTIONARY_SORT_KEYS.varname, false);
    testDictionary(conf, "orc-file-dump-dictionary-threshold-unsorted2.out");
  }

}
