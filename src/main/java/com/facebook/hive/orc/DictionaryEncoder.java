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
import java.io.OutputStream;

/**
 * A memory efficient red-black tree that does not allocate any objects per
 * an element. This class is abstract and assumes that the child class
 * handles the key and comparisons with the key.
 */
abstract class DictionaryEncoder {
  public static final int NULL = -1;
  protected final boolean sortKeys;

  /**
   * Create a set with a default initial capacity.
   */
  public DictionaryEncoder() {
    this(true);
  }

  public DictionaryEncoder(boolean sortKeys) {
    this.sortKeys = sortKeys;
  }

  /**
   * Get the number of elements in the set.
   */
  public abstract int size();

  /**
   * Reset the table to empty.
   */
  public abstract void clear();

  /**
   * Returns the amount of memory used by entries in the dictionary
   */
  public abstract int getUncompressedLength();

  /**
   * The interface for visitors.
   */
  public interface Visitor<T> {
    /**
     * Called once for each node of the tree in sort order.
     * @param context the information about each node
     * @throws IOException
     */
    void visit(VisitorContext<T> context) throws IOException;
  }

  /**
   * The information about each node.
   */
  public interface VisitorContext<T> {
    /**
     * Get the position where the key was originally added.
     * @return the number returned by add.
     */
    int getOriginalPosition();

    /**
     * Write the bytes for the string to the given output stream.
     * @param out the stream to write to.
     * @throws IOException
     */
    void writeBytes(OutputStream out) throws IOException;

    /**
     * Get the number of bytes
     * @return the string's length in bytes
     */
    int getLength();

    T getKey();

    /**
     * Get the number of times this key was added to the dictionary
     * @return the number of times this key was added to the dictionary
     */
    int getCount();

    /**
     * Get the first index stride when this key was added to the dictionary
     * @return the first index stride when this key was added to the dictionary
     */
    int getIndexStride();
  }
}

