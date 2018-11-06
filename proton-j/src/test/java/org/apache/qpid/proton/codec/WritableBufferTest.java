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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Tests for built in ByteBuffer wrapper of WritableBuffer
 */
public class WritableBufferTest {

    @Test
    public void testCreateAllocatedWrapper() {
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.allocate(10);

        assertEquals(10, buffer.remaining());
        assertEquals(0, buffer.position());
        assertTrue(buffer.hasRemaining());
    }

    @Test
    public void testCreateByteArrayWrapper() {
        byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(10, buffer.remaining());
        assertEquals(0, buffer.position());
        assertTrue(buffer.hasRemaining());
    }

    @Test
    public void testLimit() {
        ByteBuffer data = ByteBuffer.allocate(100);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(data.capacity(), buffer.limit());
    }

    @Test
    public void testRemaining() {
        ByteBuffer data = ByteBuffer.allocate(100);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(data.limit(), buffer.remaining());
        buffer.put((byte) 0);
        assertEquals(data.limit() - 1, buffer.remaining());
    }

    @Test
    public void testHasRemaining() {
        ByteBuffer data = ByteBuffer.allocate(100);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertTrue(buffer.hasRemaining());
        buffer.put((byte) 0);
        assertTrue(buffer.hasRemaining());
        data.position(data.limit());
        assertFalse(buffer.hasRemaining());
    }

    @Test
    public void testEnsureRemainingThrowsWhenExpected() {
        ByteBuffer data = ByteBuffer.allocate(100);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(data.capacity(), buffer.limit());
        try {
            buffer.ensureRemaining(1024);
            fail("Should have thrown an error on request for more than is available.");
        } catch (BufferOverflowException boe) {}

        try {
            buffer.ensureRemaining(-1);
            fail("Should have thrown an error on request for negative space.");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testEnsureRemainingDefaultImplementation() {
        WritableBuffer buffer = new DefaultWritableBuffer();

        try {
            buffer.ensureRemaining(1024);
        } catch (IndexOutOfBoundsException iobe) {
            fail("Should not have thrown an error on request for more than is available.");
        }

        try {
            buffer.ensureRemaining(-1);
        } catch (IllegalArgumentException iae) {
            fail("Should not have thrown an error on request for negative space.");
        }
    }

    @Test
    public void testGetPosition() {
        ByteBuffer data = ByteBuffer.allocate(100);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(0, buffer.position());
        data.put((byte) 0);
        assertEquals(1, buffer.position());
    }

    @Test
    public void testSetPosition() {
        ByteBuffer data = ByteBuffer.allocate(100);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(0, data.position());
        buffer.position(1);
        assertEquals(1, data.position());
    }

    @Test
    public void testPutByteBuffer() {
        ByteBuffer input = ByteBuffer.allocate(1024);
        input.put((byte) 1);
        input.flip();

        ByteBuffer data = ByteBuffer.allocate(1024);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(0, buffer.position());
        buffer.put(input);
        assertEquals(1, buffer.position());
    }

    @Test
    public void testPutString() {
        String ascii = new String("ASCII");

        ByteBuffer data = ByteBuffer.allocate(1024);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(0, buffer.position());
        buffer.put(ascii);
        assertEquals(ascii.length(), buffer.position());
    }

    @Test
    public void testPutReadableBuffer() {
        doPutReadableBufferTestImpl(true);
        doPutReadableBufferTestImpl(false);
    }

    private void doPutReadableBufferTestImpl(boolean readOnly) {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.put((byte) 1);
        buf.flip();
        if(readOnly) {
            buf = buf.asReadOnlyBuffer();
        }

        ReadableBuffer input = new ReadableBuffer.ByteBufferReader(buf);

        if(readOnly) {
            assertFalse("Expected buffer not to hasArray()", input.hasArray());
        } else {
            assertTrue("Expected buffer to hasArray()", input.hasArray());
        }

        ByteBuffer data = ByteBuffer.allocate(1024);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.wrap(data);

        assertEquals(0, buffer.position());
        buffer.put(input);
        assertEquals(1, buffer.position());
    }

    //----- WritableBuffer implementation with no default overrides ----------//

    private static class DefaultWritableBuffer implements WritableBuffer {

        private final WritableBuffer backing;

        public DefaultWritableBuffer() {
            backing = WritableBuffer.ByteBufferWrapper.allocate(1024);
        }

        @Override
        public void put(byte b) {
            backing.put(b);
        }

        @Override
        public void putFloat(float f) {
            backing.putFloat(f);
        }

        @Override
        public void putDouble(double d) {
            backing.putDouble(d);
        }

        @Override
        public void put(byte[] src, int offset, int length) {
            backing.put(src, offset, length);
        }

        @Override
        public void putShort(short s) {
            backing.putShort(s);
        }

        @Override
        public void putInt(int i) {
            backing.putInt(i);
        }

        @Override
        public void putLong(long l) {
            backing.putLong(l);
        }

        @Override
        public boolean hasRemaining() {
            return backing.hasRemaining();
        }

        @Override
        public int remaining() {
            return backing.remaining();
        }

        @Override
        public int position() {
            return backing.position();
        }

        @Override
        public void position(int position) {
            backing.position(position);
        }

        @Override
        public void put(ByteBuffer payload) {
            backing.put(payload);
        }

        @Override
        public void put(ReadableBuffer payload) {
            backing.put(payload);
        }

        @Override
        public int limit() {
            return backing.limit();
        }
    }
}
