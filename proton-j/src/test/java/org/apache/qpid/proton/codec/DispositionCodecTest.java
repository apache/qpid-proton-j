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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.transport.Disposition;
import org.apache.qpid.proton.amqp.transport.Role;
import org.junit.Test;

/**
 * Test for decoder of AMQP Disposition type.
 */
public class DispositionCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeDisposition() throws IOException {
        doTestDecodeDispositionSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfDispositions() throws IOException {
        doTestDecodeDispositionSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfDispositions() throws IOException {
        doTestDecodeDispositionSeries(LARGE_SIZE);
    }

    private void doTestDecodeDispositionSeries(int size) throws IOException {
        Disposition disposition = new Disposition();
        disposition.setRole(Role.RECEIVER);
        disposition.setFirst(UnsignedInteger.ZERO);
        disposition.setLast(UnsignedInteger.MAX_VALUE);
        disposition.setSettled(true);
        disposition.setState(Accepted.getInstance());
        disposition.setBatchable(true);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(disposition);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Disposition);

            Disposition decoded = (Disposition) result;
            assertEquals(Role.RECEIVER, decoded.getRole());
            assertEquals(UnsignedInteger.ZERO, decoded.getFirst());
            assertEquals(UnsignedInteger.MAX_VALUE, decoded.getLast());
            assertTrue(decoded.getSettled());
            assertEquals(Accepted.getInstance(), decoded.getState());
            assertTrue(decoded.getBatchable());
        }
    }

    @Test
    public void testDecodeDisposition2() throws Exception {
        Object o = doDecodeDispositionWithSpecificSizeCountTestImpl(false, 13, 3);
        assertDecodedDisposition(o);

        o = doDecodeDispositionWithSpecificSizeCountTestImpl(true, 16, 3);
        assertDecodedDisposition(o);
    }

    private void assertDecodedDisposition(Object o) {
        assertTrue(o instanceof Disposition);
        Disposition d = (Disposition) o;

        assertEquals(Role.RECEIVER, d.getRole());
        assertEquals(UnsignedInteger.ZERO, d.getFirst());
        assertEquals(UnsignedInteger.valueOf(1), d.getLast());
        assertFalse(d.getSettled());
        assertNull(d.getState());
        assertFalse(d.getBatchable());
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeDispositionWithSpecificSizeCountTestImpl(false, 13 + 1, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 14 cannot be more than remaining bytes 13", de.getMessage());
        }

        try {
            doDecodeDispositionWithSpecificSizeCountTestImpl(true, 16 + 1, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 17 cannot be more than remaining bytes 16", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeDispositionWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeDispositionWithSpecificSizeCountTestImpl(true, 3, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeDispositionWithSpecificSizeCountTestImpl(false, 2, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 3 must be less than size (2)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeDispositionWithSpecificSizeCountTestImpl(true, 6, 7);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 7 must be less than size (6)", de.getMessage());
        }
    }

    private Object doDecodeDispositionWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x15); // disposition ulong descriptor
        if (list32) {
            tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
        tmpBuf.put((byte) 0x56); // boolean
        tmpBuf.put((byte) 0x41); // true (role=receiver)
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(0); // first
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(1); // last

        tmpBuf.flip();

        int oldLimit = tmpBuf.limit();

        decoder.setByteBuffer(tmpBuf);

        final Object decoded;
        try
        {
            decoded = decoder.readObject();
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    @Test
    public void testDecodeOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeDispositionWithSpecificSizeCountTestImpl(false, 13 - 5, 3);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeDispositionWithSpecificSizeCountTestImpl(true, 16 - 5, 3);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeDispositionWithSpecificSizeCountTestImpl(false, 13, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Disposition incorrectly encoded, left 5 bytes from size indicated 13", de.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeDispositionWithSpecificSizeCountTestImpl(true, 16, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Disposition incorrectly encoded, left 5 bytes from size indicated 16", de.getMessage());
        }
    }
}
