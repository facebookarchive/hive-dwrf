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

import com.facebook.hive.orc.OrcProto.Stream.Kind;

/**
 * The name of a stream within a stripe.
 */
public class StreamName implements Comparable<StreamName> {
  private final int column;
  private final OrcProto.Stream.Kind kind;

  public static enum Area {
    DATA, DICTIONARY, INDEX
  }

  public StreamName(int column, OrcProto.Stream.Kind kind) {
    this.column = column;
    this.kind = kind;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof  StreamName) {
      StreamName other = (StreamName) obj;
      return other.column == column && other.kind == kind;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(StreamName streamName) {
    if (streamName == null) {
      return -1;
    }
    Area area = getArea(kind);
    Area otherArea = streamName.getArea(streamName.kind);
    if (area != otherArea) {
      return -area.compareTo(otherArea);
    }
    if (column != streamName.column) {
      return column < streamName.column ? -1 : 1;
    }
    return compareKinds(kind, streamName.kind);
  }

  // LENGTH is greater than DATA at the moment, but when we read the data we always read length
  // first (because you have to know how much data to read).  Since this is an enum, we're kind of
  // stuck with it, this is just a hack to work around that.
  private int compareKinds(Kind kind1, Kind kind2) {
    if (kind1 == Kind.LENGTH && kind2 == Kind.DATA) {
      return -1;
    }

    if (kind1 == Kind.DATA && kind2 == Kind.LENGTH) {
      return 1;
    }

    return kind1.compareTo(kind2);
  }

  public int getColumn() {
    return column;
  }

  public OrcProto.Stream.Kind getKind() {
    return kind;
  }

  public Area getArea() {
    return getArea(kind);
  }

  public static Area getArea(OrcProto.Stream.Kind kind) {
    switch (kind) {
      case ROW_INDEX:
      case DICTIONARY_COUNT:
        return Area.INDEX;
      case DICTIONARY_DATA:
        return Area.DICTIONARY;
      default:
        return Area.DATA;
    }
  }

  @Override
  public String toString() {
    return "Stream for column " + column + " kind " + kind;
  }

  @Override
  public int hashCode() {
    return column * 101 + kind.getNumber();
  }
}

