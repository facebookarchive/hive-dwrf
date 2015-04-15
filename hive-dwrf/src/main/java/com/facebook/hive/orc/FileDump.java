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

import com.facebook.hive.orc.compression.CompressionKind;
import com.facebook.hive.orc.statistics.ColumnStatistics;
import com.facebook.presto.hadoop.shaded.com.google.protobuf.ByteString;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {
  private static final String STREAM_SECTION_INFO_FORMAT =
      "    Stream: column %d section %s start: %d length %d";

  // not used
  private FileDump() {}

  private static void printCompressionInformation(Reader reader) {
    System.out.println("Compression: " + reader.getCompression());
    if (reader.getCompression() != CompressionKind.NONE) {
      System.out.println("Compression size: " + reader.getCompressionSize());
    }
  }

  private static void printColumnStatistics(Reader reader) {
    final ColumnStatistics[] stats = reader.getStatistics();
    System.out.println("\nStatistics:");

    for (int i = 0; i < stats.length; ++i) {
      System.out.println("  Column " + i + ": " + stats[i].toString());
    }
  }

  private static void printColumnFooterEntry(OrcProto.StripeFooter footer, int col) {
    final StringBuilder buf = new StringBuilder();
    buf.append("    Encoding column ");
    buf.append(col);
    buf.append(": ");

    final OrcProto.ColumnEncoding encoding = footer.getColumns(col);
    buf.append(encoding.getKind());

    if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY) {
      buf.append("[");
      buf.append(encoding.getDictionarySize());
      buf.append("]");
    }
    System.out.println(buf);
  }

  private static void printStripeInformation(Reader reader, RecordReaderImpl rows)
      throws IOException {
    System.out.println("\nStripes:");

    for (final StripeInformation stripe: reader.getStripes()) {
      final long stripeStart = stripe.getOffset();
      System.out.println("  Stripe: " + stripe.toString());

      final OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
      long sectionStart = stripeStart;
      for (final OrcProto.Stream section: footer.getStreamsList()) {
        System.out.println(String.format(STREAM_SECTION_INFO_FORMAT,
                                         section.getColumn(),
                                         section.getKind(),
                                         sectionStart,
                                         section.getLength()));
        sectionStart += section.getLength();
      }

      for (int col = 0; col < footer.getColumnsCount(); ++col) {
        printColumnFooterEntry(footer, col);
      }
    }
  }

  private static void printMetadataInformation(Reader reader) {
    final List<String> metadataKeys = Lists.newArrayList(reader.getMetadataKeys());
    if (!metadataKeys.isEmpty()) {
      System.out.println("\nUserMetadata:");
      for (final String key : metadataKeys) {
        final ByteBuffer storedValue = reader.getMetadataValue(key);
        final String value = ByteString.copyFrom(storedValue).toStringUtf8();
        System.out.println("\n\t" + key + " = " + value);
      }
    }
  }

  private static void processFile(String filename, Configuration conf) throws IOException {
    final Path path = new Path(filename);
    ReaderWriterProfiler.setProfilerOptions(conf);
    System.out.println("Structure for " + filename);

    final Reader reader = OrcFile.createReader(path.getFileSystem(conf), path, conf);
    final RecordReaderImpl rows = (RecordReaderImpl) reader.rows(null);
    System.out.println("Rows: " + reader.getNumberOfRows());
    printMetadataInformation(reader);
    printCompressionInformation(reader);
    System.out.println("Raw data size: " + reader.getRawDataSize());
    System.out.println("Type: " + reader.getObjectInspector().getTypeName());

    printColumnStatistics(reader);
    printStripeInformation(reader, rows);
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration();
    for(int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-hiveconf")) {
        // Skip any -hiveconf args and its values
        i++;
        continue;
      }
      processFile(args[i], conf);
    }
  }
}
