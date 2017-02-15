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

/**
 * A class that defines the constant strings used by the raw datasize calculation.
 *
 * It's shared among RC file format and ORC file format
 */
public final class RawDatasizeConst {

  public final static short NULL_SIZE = 1;

  public final static short BOOLEAN_SIZE = 1;

  public final static short BYTE_SIZE = 1;

  public final static short SHORT_SIZE = 2;

  public final static short INT_SIZE = 4;

  public final static short LONG_SIZE = 8;

  public final static short FLOAT_SIZE = 4;

  public final static short DOUBLE_SIZE = 8;

  /**
   * Raw data size is:
   *   the number of bytes needed to store the milliseconds since the epoch
   *   (8 since it's a long)
   *   +
   *   the number of bytes needed to store the nanos field (4 since it's an int)
   */
  public final static short TIMESTAMP_SIZE = 12;

  /**
   * UNION raw data size is size of tag (1) + size of value
   */
  public final static short UNION_TAG_SIZE = 1;
}
