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

package org.apache.hadoop.hive.ql.io.slice;

import static com.google.common.base.Preconditions.checkPositionIndex;

import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Objects;

@SuppressWarnings("JavaDoc") // IDEA-81310
public final class BasicSliceInput
        extends SliceInput
{
    private final Slice slice;
    private int position;

    public BasicSliceInput(Slice slice)
    {
        this.slice = slice;
    }

    @Override
    public int position()
    {
        return position;
    }

    @Override
    public void setPosition(int position)
    {
        checkPositionIndex(position, slice.length());
        this.position = position;
    }

    @Override
    public boolean isReadable()
    {
        return position < slice.length();
    }

    @Override
    public int available()
    {
        return slice.length() - position;
    }

    @Override
    public boolean readBoolean()
            throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        if (position >= slice.length()) {
            return -1;
        }
        int result = slice.getByte(position) & 0xFF;
        position++;
        return result;
    }

    @Override
    public byte readByte()
    {
        int value = read();
        if (value == -1) {
            throw new IndexOutOfBoundsException();
        }
        return (byte) value;
    }

    @Override
    public int readUnsignedByte()
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
    {
        short v = slice.getShort(position);
        position += SizeOf.SIZE_OF_SHORT;
        return v;
    }

    @Override
    public int readUnsignedShort()
            throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        int v = slice.getInt(position);
        position += SizeOf.SIZE_OF_INT;
        return v;
    }

    @Override
    public long readLong()
    {
        long v = slice.getLong(position);
        position += SizeOf.SIZE_OF_LONG;
        return v;
    }

    @Override
    public float readFloat()
    {
        float v = slice.getFloat(position);
        position += SizeOf.SIZE_OF_FLOAT;
        return v;
    }

    @Override
    public double readDouble()
    {
        double v = slice.getDouble(position);
        position += SizeOf.SIZE_OF_DOUBLE;
        return v;
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        Slice newSlice = slice.slice(position, length);
        position += length;
        return newSlice;
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        slice.getBytes(position, destination, destinationIndex, length);
        position += length;
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        slice.getBytes(position, out, length);
        position += length;
    }

    @Override
    public int skipBytes(int length)
    {
        length = Math.min(length, available());
        position += length;
        return length;
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.  This method is
     * identical to {@code buf.slice(buf.position(), buf.available()())}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public Slice slice()
    {
        return slice.slice(position, slice.length() - position);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("position", position)
                .add("capacity", slice.length())
                .toString();
    }
}
