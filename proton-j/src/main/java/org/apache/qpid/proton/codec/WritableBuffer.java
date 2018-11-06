/*
 *
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
 *
 */

package org.apache.qpid.proton.codec;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public interface WritableBuffer {
    void put(byte b);

    void putFloat(float f);

    void putDouble(double d);

    void put(byte[] src, int offset, int length);

    void putShort(short s);

    void putInt(int i);

    void putLong(long l);

    boolean hasRemaining();

    default void ensureRemaining(int requiredRemaining) {
        // No-op to allow for drop in updates
    }

    int remaining();

    int position();

    void position(int position);

    void put(ByteBuffer payload);

    void put(ReadableBuffer payload);

    default void put(final String value) {
        final int length = value.length();

        for (int i = 0; i < length; i++) {
            int c = value.charAt(i);
            if ((c & 0xFF80) == 0) {
                // U+0000..U+007F
                put((byte) c);
            } else if ((c & 0xF800) == 0)  {
                // U+0080..U+07FF
                put((byte) (0xC0 | ((c >> 6) & 0x1F)));
                put((byte) (0x80 | (c & 0x3F)));
            } else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF)) {
                // U+0800..U+FFFF - excluding surrogate pairs
                put((byte) (0xE0 | ((c >> 12) & 0x0F)));
                put((byte) (0x80 | ((c >> 6) & 0x3F)));
                put((byte) (0x80 | (c & 0x3F)));
            } else {
                int low;

                if ((++i == length) || ((low = value.charAt(i)) & 0xDC00) != 0xDC00) {
                    throw new IllegalArgumentException("String contains invalid Unicode code points");
                }

                c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                put((byte) (0xF0 | ((c >> 18) & 0x07)));
                put((byte) (0x80 | ((c >> 12) & 0x3F)));
                put((byte) (0x80 | ((c >> 6) & 0x3F)));
                put((byte) (0x80 | (c & 0x3F)));
            }
        }
    }

    int limit();

    class ByteBufferWrapper implements WritableBuffer {
        private final ByteBuffer _buf;

        public ByteBufferWrapper(ByteBuffer buf) {
            _buf = buf;
        }

        @Override
        public void put(byte b) {
            _buf.put(b);
        }

        @Override
        public void putFloat(float f) {
            _buf.putFloat(f);
        }

        @Override
        public void putDouble(double d) {
            _buf.putDouble(d);
        }

        @Override
        public void put(byte[] src, int offset, int length) {
            _buf.put(src, offset, length);
        }

        @Override
        public void putShort(short s) {
            _buf.putShort(s);
        }

        @Override
        public void putInt(int i) {
            _buf.putInt(i);
        }

        @Override
        public void putLong(long l) {
            _buf.putLong(l);
        }

        @Override
        public boolean hasRemaining() {
            return _buf.hasRemaining();
        }

        @Override
        public void ensureRemaining(int remaining) {
            if (remaining < 0) {
                throw new IllegalArgumentException("Required remaining bytes cannot be negative");
            }

            if (_buf.remaining() < remaining) {
                IndexOutOfBoundsException cause = new IndexOutOfBoundsException(String.format(
                    "Requested min remaining bytes(%d) exceeds remaining(%d) in underlying ByteBuffer: %s",
                    remaining, _buf.remaining(), _buf));

                throw (BufferOverflowException) new BufferOverflowException().initCause(cause);
            }
        }

        @Override
        public int remaining() {
            return _buf.remaining();
        }

        @Override
        public int position() {
            return _buf.position();
        }

        @Override
        public void position(int position) {
            _buf.position(position);
        }

        @Override
        public void put(ByteBuffer src) {
            _buf.put(src);
        }

        @Override
        public void put(ReadableBuffer src) {
            src.get(this);
        }

        @Override
        public void put(final String value) {
            final int length = value.length();

            int pos = _buf.position();

            for (int i = 0; i < length; i++) {
                int c = value.charAt(i);
                try {
                    if ((c & 0xFF80) == 0) {
                        // U+0000..U+007F
                        put(pos++, (byte) c);
                    } else if ((c & 0xF800) == 0)  {
                        // U+0080..U+07FF
                        put(pos++, (byte) (0xC0 | ((c >> 6) & 0x1F)));
                        put(pos++, (byte) (0x80 | (c & 0x3F)));
                    } else if ((c & 0xD800) != 0xD800 || (c > 0xDBFF))  {
                        // U+0800..U+FFFF - excluding surrogate pairs
                        put(pos++, (byte) (0xE0 | ((c >> 12) & 0x0F)));
                        put(pos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                        put(pos++, (byte) (0x80 | (c & 0x3F)));
                    } else {
                        int low;

                        if ((++i == length) || ((low = value.charAt(i)) & 0xDC00) != 0xDC00) {
                            throw new IllegalArgumentException("String contains invalid Unicode code points");
                        }

                        c = 0x010000 + ((c & 0x03FF) << 10) + (low & 0x03FF);

                        put(pos++, (byte) (0xF0 | ((c >> 18) & 0x07)));
                        put(pos++, (byte) (0x80 | ((c >> 12) & 0x3F)));
                        put(pos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                        put(pos++, (byte) (0x80 | (c & 0x3F)));
                    }
                }
                catch(IndexOutOfBoundsException ioobe) {
                    throw new BufferOverflowException();
                }
            }

            // Now move the buffer position to reflect the work done here
            _buf.position(pos);
        }

        @Override
        public int limit() {
            return _buf.limit();
        }

        public ByteBuffer byteBuffer() {
            return _buf;
        }

        public ReadableBuffer toReadableBuffer() {
            return ReadableBuffer.ByteBufferReader.wrap((ByteBuffer) _buf.duplicate().flip());
        }

        @Override
        public String toString() {
            return String.format("[pos: %d, limit: %d, remaining:%d]", _buf.position(), _buf.limit(), _buf.remaining());
        }

        public static ByteBufferWrapper allocate(int size) {
            ByteBuffer allocated = ByteBuffer.allocate(size);
            return new ByteBufferWrapper(allocated);
        }

        public static ByteBufferWrapper wrap(ByteBuffer buffer) {
            return new ByteBufferWrapper(buffer);
        }

        public static ByteBufferWrapper wrap(byte[] bytes) {
            return new ByteBufferWrapper(ByteBuffer.wrap(bytes));
        }

        private void put(int index, byte value) {
            if (_buf.hasArray()) {
                _buf.array()[_buf.arrayOffset() + index] = value;
            } else {
                _buf.put(index, value);
            }
        }
    }
}
