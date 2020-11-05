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
package org.apache.qpid.proton.codec;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.InvalidMarkException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ReadableBuffer implementation whose content is made up of one or more
 * byte arrays.
 */
public class CompositeReadableBuffer implements ReadableBuffer {

    private static final List<byte[]> EMPTY_LIST = Collections.unmodifiableList(new ArrayList<byte[]>());
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);
    private static final CompositeReadableBuffer EMPTY_SLICE = new CompositeReadableBuffer(false);
    private static int UNSET_MARK = -1;

    private static final int SHORT_BYTES = 2;
    private static final int INT_BYTES = 4;
    private static final int LONG_BYTES = 8;

    private ArrayList<byte[]> contents;

    // Track active array and our offset into it.
    private int currentArrayIndex = -1;
    private byte[] currentArray;
    private int currentOffset;

    // State global to the buffer.
    private int position;
    private int limit;
    private int capacity;
    private int mark = -1;
    private boolean compactable = true;

    /**
     * Creates a default empty composite buffer
     */
    public CompositeReadableBuffer() {
    }

    private CompositeReadableBuffer(byte[] array, int offset) {
        this.currentArray = array;
        this.currentOffset = offset;
        if(array != null) {
            this.capacity = array.length;
        }
        this.limit = capacity;
    }

    private CompositeReadableBuffer(boolean compactable) {
        this.compactable = compactable;
    }

    public List<byte[]> getArrays() {
        return contents == null ? EMPTY_LIST : Collections.unmodifiableList(contents);
    }

    public int getCurrentIndex() {
        return currentArrayIndex;
    }

    /**
     * Gets the current position index in the current backing array, which represents the current buffer position.
     *
     * This value includes any buffer position movement, and resets when moving across array segments, so it only
     * gives the starting offset for the first array if the buffer position is 0.
     *
     * Value may be out of array bounds if the the buffer currently has no content remaining.
     *
     * @return the position index in the current array representing the current buffer position.
     */
    public int getCurrentArrayPosition() {
        return currentOffset;
    }

    @Override
    public boolean hasArray() {
        return currentArray != null && (contents == null || contents.size() == 1);
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public byte[] array() {
        if (hasArray()) {
            return currentArray;
        }

        throw new UnsupportedOperationException("Buffer not backed by a single array");
    }

    @Override
    public int arrayOffset() {
        if (hasArray()) {
            return currentOffset - position;
        }

        throw new UnsupportedOperationException("Buffer not backed by a single array");
    }

    @Override
    public byte get() {
        if (position == limit) {
            throw new BufferUnderflowException();
        }

        final byte result = currentArray[currentOffset++];
        position++;
        maybeMoveToNextArray();

        return result;
    }

    @Override
    public byte get(int index) {
        if (index < 0 || index >= limit) {
            throw new IndexOutOfBoundsException("The given index is not valid: " + index);
        }

        return _get(index);
    }

    /**
     * Unchecked ie no bound-checks get
     */
    private byte _get(int index) {
        byte result = 0;

        if (index == position) {
            result = currentArray[currentOffset];
        } else if (index < position) {
            result = getBackwards(index);
        } else {
            result = getForward(index);
        }

        return result;
    }

    private byte getForward(int index) {
        byte result = 0;

        int currentArrayIndex = this.currentArrayIndex;
        int currentOffset = this.currentOffset;
        byte[] currentArray = this.currentArray;

        for (int amount = index - position; amount >= 0;) {
            if (amount < currentArray.length - currentOffset) {
                result = currentArray[currentOffset + amount];
                break;
            } else {
                amount -= currentArray.length - currentOffset;
                currentArray = contents.get(++currentArrayIndex);
                currentOffset = 0;
            }
        }

        return result;
    }

    private byte getBackwards(int index) {
        byte result = 0;

        int currentArrayIndex = this.currentArrayIndex;
        int currentOffset = this.currentOffset;
        byte[] currentArray = this.currentArray;

        for (int amount = position - index; amount >= 0;) {
            if ((currentOffset - amount) >= 0) {
                result = currentArray[currentOffset - amount];
                break;
            } else {
                amount -= currentOffset;
                currentArray = contents.get(--currentArrayIndex);
                currentOffset = currentArray.length;
            }
        }

        return result;
    }

    @Override
    public int getInt() {
        if (remaining() < INT_BYTES) {
            throw new BufferUnderflowException();
        }

        int result = 0;

        if (currentArray.length - currentOffset >= 4) {
            result = (int)(currentArray[currentOffset++] & 0xFF) << 24 |
                     (int)(currentArray[currentOffset++] & 0xFF) << 16 |
                     (int)(currentArray[currentOffset++] & 0xFF) << 8 |
                     (int)(currentArray[currentOffset++] & 0xFF) << 0;
            maybeMoveToNextArray();
        } else {
            for (int i = INT_BYTES - 1; i >= 0; --i) {
                result |= (int)(currentArray[currentOffset++] & 0xFF) << (i * Byte.SIZE);
                maybeMoveToNextArray();
            }
        }

        position += 4;

        return result;
    }

    @Override
    public long getLong() {
        if (remaining() < LONG_BYTES) {
            throw new BufferUnderflowException();
        }

        long result = 0;

        if (currentArray.length - currentOffset >= 8) {
            result = (long)(currentArray[currentOffset++] & 0xFF) << 56 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 48 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 40 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 32 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 24 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 16 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 8 |
                     (long)(currentArray[currentOffset++] & 0xFF) << 0;
            maybeMoveToNextArray();
        } else {
            for (int i = LONG_BYTES - 1; i >= 0; --i) {
                result |= (long)(currentArray[currentOffset++] & 0xFF) << (i * Byte.SIZE);
                maybeMoveToNextArray();
            }
        }

        position += 8;

        return result;
    }

    @Override
    public short getShort() {
        if (remaining() < SHORT_BYTES) {
            throw new BufferUnderflowException();
        }

        short result = 0;

        for (int i = SHORT_BYTES - 1; i >= 0; --i) {
            result |= (currentArray[currentOffset++] & 0xFF) << (i * Byte.SIZE);
            maybeMoveToNextArray();
        }

        position += 2;

        return result;
    }

    @Override
    public float getFloat() {
        return Float.intBitsToFloat(getInt());
    }

    @Override
    public double getDouble() {
        return Double.longBitsToDouble(getLong());
    }

    @Override
    public CompositeReadableBuffer get(byte[] data) {
        return get(data, 0, data.length);
    }

    @Override
    public CompositeReadableBuffer get(byte[] data, int offset, int length) {
        validateReadTarget(data.length, offset, length);

        if (length > remaining()) {
            throw new BufferUnderflowException();
        }

        int copied = 0;
        while (length > 0) {
            final int chunk = Math.min((currentArray.length - currentOffset), length);
            System.arraycopy(currentArray, currentOffset, data, offset + copied, chunk);

            currentOffset += chunk;
            length -= chunk;
            copied += chunk;

            maybeMoveToNextArray();
        }

        position += copied;

        return this;
    }

    @Override
    public CompositeReadableBuffer get(WritableBuffer target) {
        int length = Math.min(target.remaining(), remaining());

        do {
            final int chunk = Math.min((currentArray.length - currentOffset), length);

            if (chunk == 0) {
                break;  // This buffer is out of data
            }

            target.put(currentArray, currentOffset, chunk);

            currentOffset += chunk;
            position += chunk;
            length -= chunk;

            maybeMoveToNextArray();
        } while (length > 0);

        return this;
    }

    @Override
    public CompositeReadableBuffer position(int position) {
        if (position < 0 || position > limit) {
            throw new IllegalArgumentException("position must be non-negative and no greater than the limit");
        }

        int moveBy = position - this.position;
        if (moveBy >= 0) {
            moveForward(moveBy);
        } else {
            moveBackwards(Math.abs(moveBy));
        }

        this.position = position;

        if (mark > position) {
            mark = UNSET_MARK;
        }

        return this;
    }

    private void moveForward(int moveBy) {
        while (moveBy > 0) {
            if (moveBy < currentArray.length - currentOffset) {
                currentOffset += moveBy;
                break;
            } else {
                moveBy -= currentArray.length - currentOffset;
                if (currentArrayIndex != -1 && currentArrayIndex < contents.size() - 1) {
                    currentArray = contents.get(++currentArrayIndex);
                    currentOffset = 0;
                } else {
                    currentOffset = currentArray.length;
                }
            }
        }
    }

    private void moveBackwards(int moveBy) {
        while (moveBy > 0) {
            if ((currentOffset - moveBy) >= 0) {
                currentOffset -= moveBy;
                break;
            } else {
                moveBy -= currentOffset;
                currentArray = contents.get(--currentArrayIndex);
                currentOffset = currentArray.length;
            }
        }
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public CompositeReadableBuffer slice() {
        int newCapacity = limit() - position();

        final CompositeReadableBuffer result;

        if (newCapacity == 0) {
            result = EMPTY_SLICE;
        } else {
            result = new CompositeReadableBuffer(currentArray, currentOffset);
            result.contents = contents;
            result.currentArrayIndex = currentArrayIndex;
            result.capacity = newCapacity;
            result.limit = newCapacity;
            result.position = 0;
            result.compactable = false;
        }

        return result;
    }

    @Override
    public CompositeReadableBuffer flip() {
        limit = position;
        position(0); // Move by index to avoid corrupting a slice.
        mark = UNSET_MARK;

        return this;
    }

    @Override
    public CompositeReadableBuffer limit(int limit) {
        if (limit < 0 || limit > capacity) {
            throw new IllegalArgumentException("limit must be non-negative and no greater than the capacity");
        }

        if (mark > limit) {
            mark = UNSET_MARK;
        }

        if (position > limit) {
            position(limit);
        }

        this.limit = limit;

        return this;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public CompositeReadableBuffer mark() {
        this.mark = position;
        return this;
    }

    @Override
    public CompositeReadableBuffer reset() {
        if (mark < 0) {
            throw new InvalidMarkException();
        }

        position(mark);

        return this;
    }

    @Override
    public CompositeReadableBuffer rewind() {
        return position(0);
    }

    @Override
    public CompositeReadableBuffer clear() {
        mark = UNSET_MARK;
        limit = capacity;

        return position(0);
    }

    @Override
    public int remaining() {
        return limit - position;
    }

    @Override
    public boolean hasRemaining() {
        return remaining() > 0;
    }

    @Override
    public CompositeReadableBuffer duplicate() {
        CompositeReadableBuffer duplicated =
            new CompositeReadableBuffer(currentArray, currentOffset);

        if (contents != null) {
            duplicated.contents = new ArrayList<>(contents);
        }

        duplicated.capacity = capacity;
        duplicated.currentArrayIndex = currentArrayIndex;
        duplicated.limit = limit;
        duplicated.position = position;
        duplicated.mark = mark;
        duplicated.compactable = compactable;   // A slice duplicated should not allow compaction.

        return duplicated;
    }

    @Override
    public ByteBuffer byteBuffer() {
        int viewSpan = limit() - position();

        final ByteBuffer result;

        if (viewSpan == 0) {
            result = EMPTY_BUFFER;
        } else if (viewSpan <= currentArray.length - currentOffset) {
            result = ByteBuffer.wrap(currentArray, currentOffset, viewSpan);
        } else {
            result = buildByteBuffer(viewSpan);
        }

        return result.asReadOnlyBuffer();
    }

    private ByteBuffer buildByteBuffer(int span) {
        byte[] compactedView = new byte[span];
        int arrayIndex = currentArrayIndex;

        // Take whatever is left from the current array;
        System.arraycopy(currentArray, currentOffset, compactedView, 0, currentArray.length - currentOffset);
        int copied = currentArray.length - currentOffset;

        while (copied < span) {
            byte[] next = contents.get(++arrayIndex);
            final int length = Math.min(span - copied, next.length);
            System.arraycopy(next, 0, compactedView, copied, length);
            copied += length;
        }

        return ByteBuffer.wrap(compactedView);
    }

    @Override
    public String readUTF8() throws CharacterCodingException {
        return readString(StandardCharsets.UTF_8.newDecoder());
    }

    @Override
    public String readString(CharsetDecoder decoder) throws CharacterCodingException {
        if (!hasRemaining()) {
            return "";
        }

        CharBuffer decoded = null;

        if (hasArray()) {
            decoded = decoder.decode(ByteBuffer.wrap(currentArray, currentOffset, remaining()));
        } else {
            decoded = readStringFromComponents(decoder);
        }

        return decoded.toString();
    }

    private CharBuffer readStringFromComponents(CharsetDecoder decoder) throws CharacterCodingException {
        int size = (int)(remaining() * decoder.averageCharsPerByte());
        CharBuffer decoded = CharBuffer.allocate(size);

        int arrayIndex = currentArrayIndex;
        final int viewSpan = limit() - position();
        int processed = Math.min(currentArray.length - currentOffset, viewSpan);
        ByteBuffer wrapper = ByteBuffer.wrap(currentArray, currentOffset, processed);

        CoderResult step = CoderResult.OVERFLOW;

        do {
            boolean endOfInput = processed == viewSpan;
            step = decoder.decode(wrapper, decoded, endOfInput);
            if (step.isUnderflow() && endOfInput) {
                step = decoder.flush(decoded);
                break;
            }

            if (step.isOverflow()) {
                size = 2 * size + 1;
                CharBuffer upsized = CharBuffer.allocate(size);
                decoded.flip();
                upsized.put(decoded);
                decoded = upsized;
                continue;
            }

            byte[] next = contents.get(++arrayIndex);
            int wrapSize = Math.min(next.length, viewSpan - processed);
            wrapper = ByteBuffer.wrap(next, 0, wrapSize);
            processed += wrapSize;
        } while (!step.isError());

        if (step.isError()) {
            step.throwException();
        }

        return (CharBuffer) decoded.flip();
    }

    /**
     * Compact the buffer dropping arrays that have been consumed by previous
     * reads from this Composite buffer.  The limit is reset to the new capacity
     */
    @Override
    public CompositeReadableBuffer reclaimRead() {
        if (!compactable || (currentArray == null && contents == null)) {
            return this;
        }

        int totalCompaction = 0;
        int totalRemovals = 0;

        for (; totalRemovals < currentArrayIndex; ++totalRemovals) {
            byte[] element = contents.remove(0);
            totalCompaction += element.length;
        }

        currentArrayIndex -= totalRemovals;

        if (currentArray.length == currentOffset) {
            totalCompaction += currentArray.length;

            // If we are sitting on the end of the data (length == offest) then
            // we are also at the last element in the ArrayList if one is currently
            // in use, so remove the data and release the list.
            if (currentArrayIndex == 0) {
                contents.clear();
                contents = null;
            }

            currentArray = null;
            currentArrayIndex = -1;
            currentOffset = 0;
        }

        position -= totalCompaction;
        limit = capacity -= totalCompaction;

        if (mark != UNSET_MARK) {
            mark -= totalCompaction;
        }

        return this;
    }

    /**
     * Adds the given array into the composite buffer at the end.
     * <p>
     * The appended array is not copied so changes to the source array are visible in this
     * buffer and vice versa.  If this composite was empty than it would return true for the
     * {@link #hasArray()} method until another array is appended.
     * <p>
     * Calling this method resets the limit to the new capacity.
     *
     * @param array
     *      The array to add to this composite buffer.
     *
     * @throws IllegalArgumentException if the array is null or zero size.
     * @throws IllegalStateException if the buffer does not allow appends.
     *
     * @return a reference to this {@link CompositeReadableBuffer}.
     */
    public CompositeReadableBuffer append(byte[] array) {
        validateAppendable();

        if (array == null || array.length == 0) {
            throw new IllegalArgumentException("Array must not be empty or null");
        }

        if (currentArray == null) {
            currentArray = array;
            currentOffset = 0;
        } else if (contents == null) {
            contents = new ArrayList<>();
            contents.add(currentArray);
            contents.add(array);
            currentArrayIndex = 0;
            // If we exhausted the array previously then it should move to the new one now.
            maybeMoveToNextArray();
        } else {
            contents.add(array);
            // If we exhausted the list previously then it didn't move onward at the time, so it should now.
            maybeMoveToNextArray();
        }

        capacity += array.length;
        limit = capacity;

        return this;
    }

    private void validateAppendable() {
        if (!compactable) {
            throw new IllegalStateException();
        }
    }

    private void validateBuffer(ReadableBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("A non-null buffer must be provided");
        }

        if (!buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer has no remaining content to append");
        }
    }

    /**
     * Adds the given composite buffer contents (from current position, up to the limit) into this
     * composite buffer at the end. The source buffer position will be set to its limit.
     * <p>
     * The appended buffer contents are not copied wherever possible, so changes to the source
     * arrays are typically visible in this buffer and vice versa. Exceptions include where the
     * source buffer position is not located at the start of its current backing array, or where the
     * given buffer has a limit that doesn't encompass all of the last array used, and
     * so the remainder of that arrays contents must be copied first to append here.
     * <p>
     * Calling this method resets the limit to the new capacity.
     *
     * @param buffer
     *      the buffer with contents to append into this composite buffer.
     *
     * @throws IllegalArgumentException if the given buffer is null or has zero remainder.
     * @throws IllegalStateException if the buffer does not allow appends.
     *
     * @return a reference to this {@link CompositeReadableBuffer}.
     */
    public CompositeReadableBuffer append(CompositeReadableBuffer buffer) {
        validateAppendable();
        validateBuffer(buffer);

        byte[] chunk;
        do {
            int bufferRemaining = buffer.remaining();
            int arrayRemaining = buffer.currentArray.length - buffer.currentOffset;
            if (buffer.currentOffset > 0 || bufferRemaining < arrayRemaining) {
                int length = Math.min(arrayRemaining, bufferRemaining);
                chunk = new byte[length];
                System.arraycopy(buffer.currentArray, buffer.currentOffset, chunk, 0, length);
            } else {
                chunk = buffer.currentArray;
            }

            append(chunk);

            buffer.position(buffer.position() + chunk.length);
        } while (buffer.hasRemaining());

        return this;
    }

    /**
     * Adds the given readable buffer contents (from current position, up to the limit) into this
     * composite buffer at the end. The source buffer position will be set to its limit.
     * <p>
     * The appended buffer contents are not copied wherever possible, so changes to the source
     * arrays are typically visible in this buffer and vice versa. Exceptions are where the
     * source buffer is not backed by an array, or where the source buffer position is not
     * located at the start of its backing array, and so the remainder of the contents must
     * be copied first to append here.
     * <p>
     * Calling this method resets the limit to the new capacity.
     *
     * @param buffer
     *      the buffer with contents to append into this composite buffer.
     *
     * @throws IllegalArgumentException if the given buffer is null or has zero remainder.
     * @throws IllegalStateException if the buffer does not allow appends.
     *
     * @return a reference to this {@link CompositeReadableBuffer}.
     */
    public CompositeReadableBuffer append(ReadableBuffer buffer) {
        if(buffer instanceof CompositeReadableBuffer) {
            append((CompositeReadableBuffer) buffer);
        } else {
            validateAppendable();
            validateBuffer(buffer);

            if (buffer.hasArray()) {
                byte[] chunk = buffer.array();

                int bufferRemaining = buffer.remaining();
                if (buffer.arrayOffset() > 0 || bufferRemaining < chunk.length) {
                    chunk = new byte[bufferRemaining];
                    System.arraycopy(buffer.array(), buffer.arrayOffset(), chunk, 0, bufferRemaining);
                }

                append(chunk);

                buffer.position(buffer.position() + chunk.length);
            } else {
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);

                append(chunk);
            }
        }

        return this;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        int remaining = remaining();

        if (currentArrayIndex < 0 || remaining <= currentArray.length - currentOffset) {
            if (remaining > 0) {
                hash = Hashing.byteBufferCompatibleHashCode(currentArray, currentOffset, currentOffset + remaining);
            }
        } else {
            hash = hashCodeFromComponents();
        }

        return hash;
    }

    private int hashCodeFromComponents() {
        int hash = 1;
        byte[] array = currentArray;
        int arrayOffset = currentOffset;
        int arraysIndex = currentArrayIndex;

        // Run to the the array and offset where we want to start the hash from
        final int remaining = remaining();
        for (int moveBy = remaining; moveBy > 0; ) {
            if (moveBy <= array.length - arrayOffset) {
                arrayOffset += moveBy;
                break;
            } else {
                moveBy -= array.length - arrayOffset;
                array = contents.get(++arraysIndex);
                arrayOffset = 0;
            }
        }

        // Now run backwards through the arrays to match what ByteBuffer would produce
        for (int moveBy = remaining; moveBy > 0; moveBy--) {
            hash = 31 * hash + array[--arrayOffset];
            if (arrayOffset == 0 && arraysIndex > 0) {
                array = contents.get(--arraysIndex);
                arrayOffset = array.length;
            }
        }

        return hash;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof ReadableBuffer)) {
            return false;
        }

        ReadableBuffer buffer = (ReadableBuffer) other;
        final int remaining = remaining();
        if (remaining != buffer.remaining()) {
            return false;
        }

        if (remaining == 0) {
            // No content to compare, and we already checked 'remaining' is equal. Protects from NPE below.
            return true;
        }

        if (remaining <= currentArray.length - currentOffset || hasArray()) {
            // Either there is only one array, or the span to compare is within a single chunk of this buffer,
            // allowing the compare to directly access the underlying array instead of using slower get methods.
            return equals(currentArray, currentOffset, remaining, buffer);
        } else {
            return equals(this, buffer);
        }
    }

    private static boolean equals(byte[] buffer, int start, int length, ReadableBuffer other) {
        if (other.hasArray()) {
            // fast-path: jdk 11 has a vectorized Arrays::equals for ranged comparisons, but
            // sadly JDK 8 nope so let's try to save at least bound checks
            final int otherStart = other.arrayOffset() + other.position();
            return equals(buffer, start, other.array(), otherStart, length);
        } else if (other instanceof ByteBufferReader) {
            return rawEquals(buffer, start, length, other.byteBuffer());
        }
        return rawEquals(buffer, start, length, other);
    }

    private static boolean uncheckedEquals(byte[] buffer, int start, int length, CompositeReadableBuffer other) {
        final int position = other.position();
        for (int i = 0; i < length; i++) {
            if (buffer[start + i] != other._get(position + i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean uncheckedEquals(CompositeReadableBuffer buffer, ByteBuffer other, int length) {
        assert buffer.remaining() >= length;
        final int otherPosition = other.position();
        final int bufferPosition = buffer.position();
        for (int i = 0; i < length; i++) {
            if (buffer._get(bufferPosition + i) != other.get(otherPosition + i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean rawEquals(byte[] buffer, int start, int length, ByteBuffer other) {
        final int position = other.position();
        for (int i = 0; i < length; i++) {
            if (buffer[start + i] != other.get(position + i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean rawEquals(byte[] buffer, int start, int length, ReadableBuffer other) {
        final int position = other.position();
        for (int i = 0; i < length; i++) {
            if (buffer[start + i] != other.get(position + i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean equals(byte[] a, int aStart, byte[] b, int bStart, int length) {
        for (int i = 0; i < length; i++) {
            if (a[aStart + i] != b[bStart + i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean equals(CompositeReadableBuffer buffer, ReadableBuffer other) {
        final int bufferRemaining = buffer.remaining();
        if (other.hasArray()) {
            final int otherStart = other.arrayOffset() + other.position();
            // check if otherEnd is beyond other limits, because the underline array is just limited by the capacity
            if (other.limit() < otherStart + bufferRemaining) {
                throw new BufferUnderflowException();
            }
            return uncheckedEquals(other.array(), otherStart, bufferRemaining, buffer);
        }
        if (other instanceof ByteBufferReader) {
            return uncheckedEquals(buffer, other.byteBuffer(), bufferRemaining);
        }
        // slow path
        final int bufferPosition = buffer.position();
        final int otherPosition = other.position();
        for (int i = 0; i < bufferRemaining; i++) {
            if (buffer._get(bufferPosition + i) != other.get(otherPosition + i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuffer builder = new StringBuffer();
        builder.append("CompositeReadableBuffer");
        builder.append("{ pos=");
        builder.append(position());
        builder.append(" limit=");
        builder.append(limit());
        builder.append(" capacity=");
        builder.append(capacity());
        builder.append(" }");

        return builder.toString();
    }

    private void maybeMoveToNextArray() {
        if (currentArray.length == currentOffset) {
            if (currentArrayIndex >= 0 && currentArrayIndex < (contents.size() - 1)) {
                currentArray = contents.get(++currentArrayIndex);
                currentOffset = 0;
            }
        }
    }

    private static void validateReadTarget(int destSize, int offset, int length) {
        if ((offset | length) < 0) {
            throw new IndexOutOfBoundsException("offset and legnth must be non-negative");
        }

        if (((long) offset + (long) length) > destSize) {
            throw new IndexOutOfBoundsException("target is to small for specified read size");
        }
    }
}
