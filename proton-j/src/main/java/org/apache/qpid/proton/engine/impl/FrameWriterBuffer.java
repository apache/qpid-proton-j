/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton.engine.impl;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FrameWriterBuffer implements WritableBuffer {

    public final static int DEFAULT_CAPACITY = 1024;

    byte array[];
    int position;

   /**
    * Creates a new WritableBuffer with default capacity.
    */
   public FrameWriterBuffer() {
       this(DEFAULT_CAPACITY);
   }

    /**
     * Create a new WritableBuffer with the given capacity.
     *
     * @param capacity
     *      the inital capacity to allocate for this buffer.
     */
    public FrameWriterBuffer(int capacity) {
        this.array = new byte[capacity];
    }

    public byte[] array() {
        return array;
    }

    public int arrayOffset() {
        return 0;
    }

    @Override
    public void put(byte b) {
        ensureRemaining(Byte.BYTES);
        array[position++] = b;
    }

    @Override
    public void putShort(short value) {
        ensureRemaining(Short.BYTES);
        array[position++] = (byte)(value >>> 8);
        array[position++] = (byte)(value >>> 0);
    }

    @Override
    public void putInt(int value) {
        ensureRemaining(Integer.BYTES);
        array[position++] = (byte)(value >>> 24);
        array[position++] = (byte)(value >>> 16);
        array[position++] = (byte)(value >>> 8);
        array[position++] = (byte)(value >>> 0);
    }

    @Override
    public void putLong(long value) {
        ensureRemaining(Long.BYTES);
        array[position++] = (byte)(value >>> 56);
        array[position++] = (byte)(value >>> 48);
        array[position++] = (byte)(value >>> 40);
        array[position++] = (byte)(value >>> 32);
        array[position++] = (byte)(value >>> 24);
        array[position++] = (byte)(value >>> 16);
        array[position++] = (byte)(value >>> 8);
        array[position++] = (byte)(value >>> 0);
    }

    @Override
    public void putFloat(float value) {
        putInt(Float.floatToRawIntBits(value));
    }

    @Override
    public void putDouble(double value) {
        putLong(Double.doubleToRawLongBits(value));
    }

    @Override
    public void put(byte[] src, int offset, int length) {
        if (length == 0) {
            return;
        }

        ensureRemaining(length);
        System.arraycopy(src, offset, array, position, length);
        position += length;
    }

    @Override
    public void put(ByteBuffer payload) {
        final int toCopy = payload.remaining();
        ensureRemaining(toCopy);

        if (payload.hasArray()) {
            System.arraycopy(payload.array(), payload.arrayOffset() + payload.position(), array, position, toCopy);
            payload.position(payload.position() + toCopy);
        } else {
            payload.get(array, position, toCopy);
        }

        position += toCopy;
    }

    @Override
    public void put(ReadableBuffer payload) {
        final int toCopy = payload.remaining();
        ensureRemaining(toCopy);

        if (payload.hasArray()) {
            System.arraycopy(payload.array(), payload.arrayOffset() + payload.position(), array, position, toCopy);
            payload.position(payload.position() + toCopy);
        } else {
            payload.get(array, position, toCopy);
        }

        position += toCopy;
    }

    @Override
    public boolean hasRemaining() {
        return position < Integer.MAX_VALUE;
    }

    @Override
    public int remaining() {
        return Integer.MAX_VALUE - position;
    }

    /**
     * Ensures the the buffer has at least the requiredRemaining space specified.
     * <p>
     * The internal buffer will be doubled if the requested capacity is less than that
     * amount or the buffer will be expanded to the full new requiredRemaining value.
     *
     * @param requiredRemaining
     *      the minimum remaining bytes needed to meet the next write operation.
     */
    @Override
    public void ensureRemaining(int requiredRemaining) {
        if (requiredRemaining > array.length - position) {
            byte newBuffer[] = new byte[Math.max(array.length << 1, requiredRemaining + position)];
            System.arraycopy(array, 0, newBuffer, 0, array.length);
            array = newBuffer;
        }
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public void position(int position) {
        if (position < 0) {
            throw new IllegalArgumentException("Requested new buffer position cannot be negative");
        }

        if (position > array.length) {
            ensureRemaining(position - array.length);
        }

        this.position = position;
    }

    @Override
    public int limit() {
        return Integer.MAX_VALUE;
    }

    /**
     * Copy bytes from this buffer into the target buffer and compacts this buffer.
     * <p>
     * Copy either all bytes written into this buffer (start to current position) or
     * as many as will fit if the target capacity is less that the bytes written.  Bytes
     * not read from this buffer are moved to the front of the buffer and the position is
     * reset to the end of the copied region.
     *
     * @param target
     *      The array to move bytes to from those written into this buffer.
     *
     * @return the number of bytes transfered to the target buffer.
     */
    public int transferTo(ByteBuffer target) {
        int size = Math.min(position, target.remaining());

        if (size == 0) {
            return 0;
        }

        if (target.hasArray()) {
            System.arraycopy(array, 0, target.array(), target.arrayOffset() + target.position(), size);
            target.position(target.position() + size);
        } else {
            target.put(array, 0, size);
        }

        // Compact any remaining data to the front of the array so that new writes can reuse
        // space previously allocated and not extend the array if possible.
        if (size != position) {
            int remainder = position - size;
            System.arraycopy(array, size, array, 0, remainder);
            position = remainder;  // ensure we are at end of unread chunk
        } else {
            position = 0; // reset to empty state.
        }

        return size;
    }
}
