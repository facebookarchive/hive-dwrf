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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.facebook.hive.ql.io.orc.OrcFile.KeyWrapper;
import com.facebook.hive.ql.io.orc.OrcFile.ValueWrapper;

class StripeReader {
  private final FSDataInputStream file;
  private final List<StripeInformation> stripes =
    new ArrayList<StripeInformation>();
  private StripeInformation currentStripe;
  private byte[] currentData;
  private int stripesRead = 0;

  StripeReader(Iterable<StripeInformation> stripes,
                   FileSystem fileSystem,
                   Path path,
                   long offset, long length
                  ) throws IOException {
    this.file = fileSystem.open(path);
    for(StripeInformation stripe: stripes) {
      long stripeStart = stripe.getOffset();
      if (stripeStart >= offset && stripeStart < offset + length) {
        this.stripes.add(stripe);
      }
    }
  }

  private void readStripe() throws IOException {
    currentStripe = stripes.get(stripesRead);
    currentData = new byte[(int) (currentStripe.getIndexLength() + currentStripe.getDataLength() +
          currentStripe.getFooterLength())];
    file.seek(currentStripe.getOffset());
    file.readFully(currentData, 0, currentData.length);
  }

  public boolean hasNext() throws IOException {
    return stripesRead < stripes.size();
  }

  public boolean nextStripe(KeyWrapper keyWrapper, ValueWrapper valueWrapper) throws IOException {
    if (hasNext()) {
      readStripe();
      keyWrapper.key = currentStripe;
      valueWrapper.value = currentData;
      stripesRead += 1;
    }
    return hasNext();
  }

  public void close() throws IOException {
    file.close();
  }

  public long getPosition() throws IOException {
    return file.getPos();
  }

  /**
   * Return the fraction of stripes that have been read from the selected.
   * section of the file
   * @return fraction between 0.0 and 1.0 of stripes consumed
   */
  public float getProgress() {
    return ((float) stripesRead) / stripes.size();
  }
}
