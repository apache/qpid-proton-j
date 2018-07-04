/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.proton.codec;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Interface to abstract a buffer, similar to {@link WritableBuffer}
 */
public interface ReadableBuffer {

    /**
     * Returns the capacity of the backing buffer of this ReadableBuffer
     * @return the capacity of the backing buffer of this ReadableBuffer
     */
    int capacity();

    /**
     * Returns true if this ReadableBuffer is backed by an array which can be
     * accessed by the {@link #array()} and {@link #arrayOffset()} methods.
     *
     * @return true if the buffer is backed by a primitive array.
     */
    boolean hasArray();

    /**
     * Returns the primitive array that backs this buffer if one exists and the
     * buffer is not read-only.  The caller should have checked the {@link #hasArray()}
     * method before calling this method.
     *
     * @return the array that backs this buffer is available.
     *
     * @throws UnsupportedOperationException if this {@link ReadableBuffer} doesn't support array access.
     * @throws ReadOnlyBufferException if the ReadableBuffer is read-only.
     */
    byte[] array();

    /**
     * Returns the offset into the backing array of the first element in the buffer. The caller
     * should have checked the {@link #hasArray()} method before calling this method.
     *
     * @return the offset into the backing array of the first element in the buffer.
     *
     * @throws UnsupportedOperationException if this {@link ReadableBuffer} doesn't support array access.
     * @throws ReadOnlyBufferException if the ReadableBuffer is read-only.
     */
    int arrayOffset();

    /**
     * Compact the backing storage of this ReadableBuffer, possibly freeing previously-read
     * portions of pooled data or reducing the number of backing arrays if present.
     * <p>
     * This is an optional operation and care should be taken in its implementation.
     *
     * @return a reference to this buffer
     */
    ReadableBuffer reclaimRead();

    /**
     * Reads the byte at the current position and advances the position by 1.
     *
     * @return the byte at the current position.
     *
     * @throws BufferUnderflowException if the buffer position has reached the limit.
     */
    byte get();

    /**
     * Reads the byte at the given index, the buffer position is not affected.
     *
     * @param index
     *      The index in the buffer from which to read the byte.
     *
     * @return the byte value stored at the target index.
     *
     * @throws IndexOutOfBoundsException if the index is not in range for this buffer.
     */
    byte get(int index);

    /**
     * Reads four bytes from the buffer and returns them as an integer value.  The
     * buffer position is advanced by four byes.
     *
     * @return and integer value composed of bytes read from the buffer.
     *
     * @throws BufferUnderflowException if the buffer position has reached the limit.
     */
    int getInt();

    /**
     * Reads eight bytes from the buffer and returns them as an long value.  The
     * buffer position is advanced by eight byes.
     *
     * @return and long value composed of bytes read from the buffer.
     *
     * @throws BufferUnderflowException if the buffer position has reached the limit.
     */
    long getLong();

    /**
     * Reads two bytes from the buffer and returns them as an short value.  The
     * buffer position is advanced by two byes.
     *
     * @return and short value composed of bytes read from the buffer.
     *
     * @throws BufferUnderflowException if the buffer position has reached the limit.
     */
    short getShort();

    /**
     * Reads four bytes from the buffer and returns them as an float value.  The
     * buffer position is advanced by four byes.
     *
     * @return and float value composed of bytes read from the buffer.
     *
     * @throws BufferUnderflowException if the buffer position has reached the limit.
     */
    float getFloat();

    /**
     * Reads eight bytes from the buffer and returns them as an double value.  The
     * buffer position is advanced by eight byes.
     *
     * @return and double value composed of bytes read from the buffer.
     *
     * @throws BufferUnderflowException if the buffer position has reached the limit.
     */
    double getDouble();

    /**
     * A bulk read method that copies bytes from this buffer into the target byte array.
     *
     * @param target
     *      The byte array to copy bytes read from this buffer.
     * @param offset
     *      The offset into the given array where the copy starts.
     * @param length
     *      The number of bytes to copy into the target array.
     *
     * @return a reference to this ReadableBuffer instance.
     *
     * @throws BufferUnderflowException if the are less readable bytes than the array length.
     * @throws IndexOutOfBoundsException if the offset or length values are invalid.
     */
    ReadableBuffer get(final byte[] target, final int offset, final int length);

    /**
     * A bulk read method that copies bytes from this buffer into the target byte array.
     *
     * @param target
     *      The byte array to copy bytes read from this buffer.
     *
     * @return a reference to this ReadableBuffer instance.
     *
     * @throws BufferUnderflowException if the are less readable bytes than the array length.
     */
    ReadableBuffer get(final byte[] target);

    /**
     * Copy data from this buffer to the target buffer starting from the current
     * position and continuing until either this buffer's remaining bytes are
     * consumed or the target is full.
     *
     * @param target
     *      The WritableBuffer to transfer this buffer's data to.
     *
     * @return a reference to this ReadableBuffer instance.
     */
    ReadableBuffer get(WritableBuffer target);

    /**
     * Creates a new ReadableBuffer instance that is a view of the readable portion of
     * this buffer.  The position will be set to zero and the limit and the reported capacity
     * will match the value returned by this buffer's {@link #remaining()} method, the mark
     * will be undefined.
     *
     * @return a new ReadableBuffer that is a view of the readable portion of this buffer.
     */
    ReadableBuffer slice();

    /**
     * Sets the buffer limit to the current position and the position is set to zero, the
     * mark value reset to undefined.
     *
     * @return a reference to this {@link ReadableBuffer}.
     */
    ReadableBuffer flip();

    /**
     * Sets the current read limit of this buffer to the given value.  If the buffer mark
     * value is defined and is larger than the limit the mark will be discarded.  If the
     * position is larger than the new limit it will be reset to the new limit.
     *
     * @param limit
     *      The new read limit to set for this buffer.
     *
     * @return a reference to this {@link ReadableBuffer}.
     *
     * @throws IllegalArgumentException if the limit value is negative or greater than the capacity.
     */
    ReadableBuffer limit(int limit);

    /**
     * @return the current value of this buffer's limit.
     */
    int limit();

    /**
     * Sets the current position of this buffer to the given value.  If the buffer mark
     * value is defined and is larger than the newly set position is must be discarded.
     *
     * @param position
     *      The new position to set for this buffer.
     *
     * @return a reference to this {@link ReadableBuffer}.
     *
     * @throws IllegalArgumentException if the position value is negative or greater than the limit.
     */
    ReadableBuffer position(int position);

    /**
     * @return the current position from which the next read operation will start.
     */
    int position();

    /**
     * Mark the current position of this buffer which can be returned to after a
     * read operation by calling {@link #reset()}.
     *
     * @return a reference to this {@link ReadableBuffer}.
     */
    ReadableBuffer mark();

    /**
     * Reset the buffer's position to a previously marked value, the mark should remain
     * set after calling this method.
     *
     * @return a reference to this {@link ReadableBuffer}.
     *
     * @throws InvalidMarkException if the mark value is undefined.
     */
    ReadableBuffer reset();

    /**
     * Resets the buffer position to zero and clears and previously set mark.
     *
     * @return a reference to this {@link ReadableBuffer}.
     */
    ReadableBuffer rewind();

    /**
     * Resets the buffer position to zero and sets the limit to the buffer capacity,
     * the mark value is discarded if set.
     *
     * @return a reference to this {@link ReadableBuffer}.
     */
    ReadableBuffer clear();

    /**
     * @return the remaining number of readable bytes in this buffer.
     */
    int remaining();

    /**
     * @return true if there are readable bytes still remaining in this buffer.
     */
    boolean hasRemaining();

    /**
     * Creates a duplicate {@link ReadableBuffer} to this instance.
     * <p>
     * The duplicated buffer will have the same position, limit and mark as this
     * buffer.  The two buffers share the same backing data.
     *
     * @return a duplicate of this {@link ReadableBuffer}.
     */
    ReadableBuffer duplicate();

    /**
     * @return a ByteBuffer view of the current readable portion of this buffer.
     */
    ByteBuffer byteBuffer();

    /**
     * Reads a UTF-8 encoded String from the buffer starting the decode at the
     * current position and reading until the limit is reached.  The position
     * is advanced to the limit once this method returns.  If there is no bytes
     * remaining in the buffer when this method is called a null is returned.
     *
     * @return a string decoded from the remaining bytes in this buffer.
     *
     * @throws CharacterCodingException if the encoding is invalid for any reason.
     */
    String readUTF8() throws CharacterCodingException;

    /**
     * Decodes a String from the buffer using the provided {@link CharsetDecoder}
     * starting the decode at the current position and reading until the limit is
     * reached.  The position is advanced to the limit once this method returns.
     * If there is no bytes remaining in the buffer when this method is called a
     * null is returned.
     *
     * @return a string decoded from the remaining bytes in this buffer.
     *
     * @throws CharacterCodingException if the encoding is invalid for any reason.
     */
    String readString(CharsetDecoder decoder) throws CharacterCodingException;

    final class ByteBufferReader implements ReadableBuffer {

        private ByteBuffer buffer;

        public static ByteBufferReader allocate(int size) {
            ByteBuffer allocated = ByteBuffer.allocate(size);
            return new ByteBufferReader(allocated);
        }

        public static ByteBufferReader wrap(ByteBuffer buffer) {
            return new ByteBufferReader(buffer);
        }

        public static ByteBufferReader wrap(byte[] array) {
            return new ByteBufferReader(ByteBuffer.wrap(array));
        }

        public ByteBufferReader(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int capacity() {
            return buffer.capacity();
        }

        @Override
        public byte get() {
            return buffer.get();
        }

        @Override
        public byte get(int index) {
            return buffer.get(index);
        }

        @Override
        public int getInt() {
            return buffer.getInt();
        }

        @Override
        public long getLong() {
            return buffer.getLong();
        }

        @Override
        public short getShort() {
            return buffer.getShort();
        }

        @Override
        public float getFloat() {
            return buffer.getFloat();
        }

        @Override
        public double getDouble() {
            return buffer.getDouble();
        }

        @Override
        public int limit() {
            return buffer.limit();
        }

        @Override
        public ReadableBuffer get(byte[] data, int offset, int length) {
            buffer.get(data, offset, length);
            return this;
        }

        @Override
        public ReadableBuffer get(byte[] data) {
            buffer.get(data);
            return this;
        }

        @Override
        public ReadableBuffer flip() {
            buffer.flip();
            return this;
        }

        @Override
        public ReadableBuffer position(int position) {
            buffer.position(position);
            return this;
        }

        @Override
        public ReadableBuffer slice() {
            return new ByteBufferReader(buffer.slice());
        }

        @Override
        public ReadableBuffer limit(int limit) {
            buffer.limit(limit);
            return this;
        }

        @Override
        public int remaining() {
            return buffer.remaining();
        }

        @Override
        public int position() {
            return buffer.position();
        }

        @Override
        public boolean hasRemaining() {
            return buffer.hasRemaining();
        }

        @Override
        public ReadableBuffer duplicate() {
            return new ByteBufferReader(buffer.duplicate());
        }

        @Override
        public ByteBuffer byteBuffer() {
            return buffer;
        }

        @Override
        public String readUTF8() {
            return StandardCharsets.UTF_8.decode(buffer).toString();
        }

        @Override
        public String readString(CharsetDecoder decoder) throws CharacterCodingException {
            return decoder.decode(buffer).toString();
        }

        @Override
        public boolean hasArray() {
            return buffer.hasArray();
        }

        @Override
        public byte[] array() {
            return buffer.array();
        }

        @Override
        public int arrayOffset() {
            return buffer.arrayOffset();
        }

        @Override
        public ReadableBuffer reclaimRead() {
            // Don't compact ByteBuffer due to the expense of the copy
            return this;
        }

        @Override
        public ReadableBuffer mark() {
            buffer.mark();
            return this;
        }

        @Override
        public ReadableBuffer reset() {
            buffer.reset();
            return this;
        }

        @Override
        public ReadableBuffer rewind() {
            buffer.rewind();
            return this;
        }

        @Override
        public ReadableBuffer clear() {
            buffer.clear();
            return this;
        }

        @Override
        public ReadableBuffer get(WritableBuffer target) {
            target.put(buffer);
            return this;
        }

        @Override
        public String toString() {
            return buffer.toString();
        }

        @Override
        public int hashCode() {
            return buffer.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof ReadableBuffer)) {
                return false;
            }

            ReadableBuffer readable = (ReadableBuffer) other;
            if (this.remaining() != readable.remaining()) {
                return false;
            }

            if (other instanceof CompositeReadableBuffer) {
                return other.equals(this);
            }

            return buffer.equals(readable.byteBuffer());
        }
    }
}