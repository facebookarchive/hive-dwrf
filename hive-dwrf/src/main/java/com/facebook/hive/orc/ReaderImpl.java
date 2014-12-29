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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.facebook.hive.orc.compression.CompressionCodec;
import com.facebook.hive.orc.compression.CompressionKind;
import com.facebook.hive.orc.statistics.ColumnStatistics;
import com.facebook.hive.orc.statistics.ColumnStatisticsImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.facebook.hive.orc.lazy.OrcLazyRowObjectInspector;
import com.google.protobuf.CodedInputStream;
import org.apache.hadoop.io.IOUtils;

public final class ReaderImpl implements Reader {
  private static final Log LOG = LogFactory.getLog(ReaderImpl.class);
  private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

  private final FileSystem fileSystem;
  private final Path path;
  private final Configuration conf;
  private final CompressionKind compressionKind;
  private final CompressionCodec codec;
  private final int bufferSize;
  private final OrcProto.Footer footer;
  private final ObjectInspector inspector;

  private static class StripeInformationImpl
      implements StripeInformation {
    private final OrcProto.StripeInformation stripe;

    StripeInformationImpl(OrcProto.StripeInformation stripe) {
      this.stripe = stripe;
    }

    @Override
    public long getOffset() {
      return stripe.getOffset();
    }

    @Override
    public long getDataLength() {
      return stripe.getDataLength();
    }

    @Override
    public long getFooterLength() {
      return stripe.getFooterLength();
    }

    @Override
    public long getIndexLength() {
      return stripe.getIndexLength();
    }

    @Override
    public long getNumberOfRows() {
      return stripe.getNumberOfRows();
    }

    @Override
    public long getRawDataSize() {
      return stripe.getRawDataSize();
    }

    @Override
    public String toString() {
      return "offset: " + getOffset() + " data: " + getDataLength() +
        " rows: " + getNumberOfRows() + " tail: " + getFooterLength() +
        " index: " + getIndexLength() + " raw_data: " + getRawDataSize();
    }
  }

  @Override
  public long getNumberOfRows() {
    return footer.getNumberOfRows();
  }

  @Override
  public long getRawDataSize() {
    return footer.getRawDataSize();
  }

  @Override
  public Iterable<String> getMetadataKeys() {
    List<OrcProto.UserMetadataItem> metadata = footer.getMetadataList();
    List<String> result = new ArrayList<String>(metadata.size());

    for(OrcProto.UserMetadataItem item: metadata) {
      result.add(item.getName());
    }
    return result;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    for(OrcProto.UserMetadataItem item: footer.getMetadataList()) {
      if (item.hasName() && item.getName().equals(key)) {
        return item.getValue().asReadOnlyByteBuffer();
      }
    }
    throw new IllegalArgumentException("Can't find user metadata " + key);
  }

  @Override
  public CompressionKind getCompression() {
    return compressionKind;
  }

  @Override
  public int getCompressionSize() {
    return bufferSize;
  }

  @Override
  public Iterable<StripeInformation> getStripes() {
    return new Iterable<com.facebook.hive.orc.StripeInformation>(){

      @Override
      public Iterator<com.facebook.hive.orc.StripeInformation> iterator() {
        return new Iterator<com.facebook.hive.orc.StripeInformation>(){
          private final Iterator<OrcProto.StripeInformation> inner =
            footer.getStripesList().iterator();

          @Override
          public boolean hasNext() {
            return inner.hasNext();
          }

          @Override
          public com.facebook.hive.orc.StripeInformation next() {
            return new StripeInformationImpl(inner.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("remove unsupported");
          }
        };
      }
    };
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }

  @Override
  public long getContentLength() {
    return footer.getContentLength();
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return footer.getTypesList();
  }

  @Override
  public int getRowIndexStride() {
    return footer.getRowIndexStride();
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    ColumnStatistics[] result = new ColumnStatistics[footer.getTypesCount()];
    for(int i=0; i < result.length; ++i) {
      result[i] = ColumnStatisticsImpl.deserialize(footer.getStatistics(i));
    }
    return result;
  }

  public ReaderImpl(FileSystem fs, Path path, Configuration conf) throws IOException {
    try {
      this.fileSystem = fs;
      this.path = path;
      this.conf = conf;
      FSDataInputStream file = fs.open(path);
      long size = fs.getFileStatus(path).getLen();
      int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
      ByteBuffer buffer = ByteBuffer.allocate(readSize);
      InStream.read(
          file, size - readSize, buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
      int psLen = buffer.get(readSize - 1);
      int psOffset = readSize - 1 - psLen;
      CodedInputStream in = CodedInputStream.newInstance(
          buffer.array(),
          buffer.arrayOffset() + psOffset, psLen);
      OrcProto.PostScript ps = OrcProto.PostScript.parseFrom(in);
      int footerSize = (int) ps.getFooterLength();
      bufferSize = (int) ps.getCompressionBlockSize();
      switch (ps.getCompression()) {
      case NONE:
        compressionKind = CompressionKind.NONE;
        break;
      case ZLIB:
        compressionKind = CompressionKind.ZLIB;
        break;
      case SNAPPY:
        compressionKind = CompressionKind.SNAPPY;
        break;
      case LZO:
        compressionKind = CompressionKind.LZO;
        break;
      default:
        throw new IllegalArgumentException("Unknown compression");
      }
      codec = WriterImpl.createCodec(compressionKind);

      InputStream instream = InStream.create(
          "footer", file, size - 1 - psLen - footerSize, footerSize,
          codec, bufferSize);
      footer = OrcProto.Footer.parseFrom(instream);
      inspector = new OrcLazyRowObjectInspector(0, footer.getTypesList());
      file.close();
    } catch (IndexOutOfBoundsException e) {
      /**
       * When a non ORC file is read by ORC reader, we get IndexOutOfBoundsException exception while
       * creating a reader. Caught that exception and checked the file header to see if the input
       * file was ORC or not. If its not ORC, throw a NotAnORCFileException with the file
       * attempted to be reading (thus helping to figure out which table-partition was being read).
       */
      checkIfORC(fs, path);
      throw new IOException("Failed to create record reader for file " + path , e);
    } catch (IOException e) {
      throw new IOException("Failed to create record reader for file " + path , e);
    }
  }

  /**
   * Reads the file header (first 40 bytes) and checks if the first three characters are 'ORC'.
   */
  public static void checkIfORC(FileSystem fs, Path path) throws IOException {
    // hardcoded to 40 because "SEQ-org.apache.hadoop.hive.ql.io.RCFile", the header, is of 40 chars
    final int buffLen = 40;
    final byte header[] = new byte[buffLen];
    final FSDataInputStream file = fs.open(path);
    final long fileLength = fs.getFileStatus(path).getLen();
    int sizeToBeRead = buffLen;
    if (buffLen > fileLength) {
      sizeToBeRead = (int)fileLength;
    }

    IOUtils.readFully(file, header, 0, sizeToBeRead);
    file.close();

    final String headerString = new String(header);
    if (headerString.startsWith("ORC")) {
      LOG.error("Error while parsing the footer of the file : " + path);
    } else {
      throw new NotAnORCFileException("Input file = " + path + " , header = " + headerString);
    }
  }

  @Override
  public RecordReader rows(boolean[] include) throws IOException {
    return rows(0, Long.MAX_VALUE, include);
  }

  @Override
  public RecordReader rows(long offset, long length, boolean[] include
                           ) throws IOException {
    return new RecordReaderImpl(this.getStripes(), fileSystem,  path, offset,
      length, footer.getTypesList(), codec, bufferSize,
      include, footer.getRowIndexStride(), conf);
  }

  @Override
  public StripeReader stripes(long offset, long length) throws IOException {
    return new StripeReader(this.getStripes(), fileSystem, path, offset, length);
  }

}
