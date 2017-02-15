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

package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcStruct;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;

public class OrcLazyRow extends OrcLazyStruct {

  private OrcLazyObject[] fields;
  private final List<String> fieldNames;

  public OrcLazyRow(OrcLazyObject[] fields, List<String> fieldNames) {
    super(null);
    this.fields = fields;
    this.fieldNames = fieldNames;
  }

  @Override
  public void next() {
    super.next();
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.next();
      }
    }
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.startStripe(streams, encodings, indexes, rowBaseInStripe);
      }
    }
  }

  @Override
  public Object materialize(long row, Object previous) throws IOException {
    OrcStruct previousRow;
    if (previous != null) {
      previousRow = (OrcStruct) previous;
      previousRow.setFieldNames(fieldNames);
    } else {
      previousRow = new OrcStruct(fieldNames);
    }
    for (int i = 0; i < fields.length; i++) {
      previousRow.setFieldValue(i, fields[i]);
    }
    return previousRow;
  }

  @Override
  public void seekToRow(long rowNumber) throws IOException {
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.seekToRow(rowNumber);
      }
    }
  }

  public int getNumFields() {
    return fields.length;
  }

  public OrcLazyObject getFieldValue(int index) {
    if (index >= fields.length) {
      return null;
    }

    return fields[index];
  }

  public void reset(OrcLazyRow other) throws IOException {
    this.fields = other.getRawFields();
    seekToRow(0);
  }

  protected OrcLazyObject[] getRawFields() {
    return fields;
  }

  @Override
  public void close() throws IOException {
    for (OrcLazyObject field : fields) {
      if (field != null) {
        field.close();
      }
    }
  }
}
