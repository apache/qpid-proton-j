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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.junit.Test;

/**
 * Test for decoder of AMQP Header type.
 */
public class HeaderTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024 * 10;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeHeader() throws IOException {
        doTestDecodeHeaderSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {
        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority(UnsignedByte.valueOf((byte) 3));
        header.setDeliveryCount(UnsignedInteger.valueOf(10));
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTtl(UnsignedInteger.valueOf(500));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(header);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Header);

            Header decoded = (Header) result;

            assertEquals(3, decoded.getPriority().intValue());
            assertTrue(decoded.getDurable().booleanValue());
        }
    }

    @Test
    public void testSkipHeader() throws IOException {
        Header header1 = new Header();
        Header header2 = new Header();

        header1.setDurable(Boolean.FALSE);
        header2.setDurable(Boolean.TRUE);

        encoder.writeObject(header1);
        encoder.writeObject(header2);

        buffer.clear();

        TypeConstructor<?> headerType = decoder.readConstructor();
        assertEquals(Header.class, headerType.getTypeClass());
        headerType.skipValue();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof Header);

        Header decoded = (Header) result;
        assertTrue(decoded.getDurable().booleanValue());
    }

    @Test
    public void testDecodeHeaderArray() throws IOException {
        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority(UnsignedByte.valueOf((byte) 3));
        header.setDeliveryCount(UnsignedInteger.valueOf(10));
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTtl(UnsignedInteger.valueOf(500));

        Header[] source = new Header[32];

        for (int i = 0; i < source.length; ++i) {
            source[i] = header;
        }

        encoder.writeObject(source);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        final Object[] resultArray = (Object[]) result;

        for (int i = 0; i < source.length; ++i) {

            assertTrue(resultArray[i] instanceof Header);

            Header decoded = (Header) resultArray[i];

            assertEquals(3, decoded.getPriority().intValue());
            assertTrue(decoded.getDurable().booleanValue());
            assertEquals(header.getDeliveryCount(), decoded.getDeliveryCount());
        }
    }

    @Test
    public void testDecodeHeader2() throws Exception {
        Object o = doDecodeHeaderWithSpecificSizeCountTestImpl(false, 5, 2);
        assertDecodedHeader(o);

        o = doDecodeHeaderWithSpecificSizeCountTestImpl(true, 8, 2);
        assertDecodedHeader(o);
    }

    private void assertDecodedHeader(Object o) {
        assertTrue(o instanceof Header);
        Header h = (Header) o;

        assertTrue(h.getDurable());
        assertEquals(UnsignedByte.valueOf((byte) 5), h.getPriority());
        assertNull(h.getTtl());
        assertNull(h.getDeliveryCount());
        assertNull(h.getFirstAcquirer());
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeHeaderWithSpecificSizeCountTestImpl(false, 5 + 1, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 6 cannot be more than remaining bytes 5", de.getMessage());
        }

        try {
            doDecodeHeaderWithSpecificSizeCountTestImpl(true, 8 + 1, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 9 cannot be more than remaining bytes 8", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeHeaderWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeHeaderWithSpecificSizeCountTestImpl(true, 2, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeHeaderWithSpecificSizeCountTestImpl(false, 2, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 3 must be less than size (2)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeHeaderWithSpecificSizeCountTestImpl(true, 6, 7);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 7 must be less than size (6)", de.getMessage());
        }
    }

    private Object doDecodeHeaderWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x70); // header ulong descriptor
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
        tmpBuf.put((byte) 0x41); // durable = true
        tmpBuf.put((byte) 0x50); // ubyte
        tmpBuf.put((byte) 5); // priority = 5

        tmpBuf.flip();

        decoder.setByteBuffer(tmpBuf);

        return decoder.readObject();
    }

    @Test
    public void testDecodeOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeHeaderWithSpecificSizeCountTestImpl(false, 5 - 2, 2);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeHeaderWithSpecificSizeCountTestImpl(true, 8 - 2, 2);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeHeaderWithSpecificSizeCountTestImpl(false, 5, 2 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Header incorrectly encoded, left 2 bytes from size indicated 5", de.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeHeaderWithSpecificSizeCountTestImpl(true, 8, 2 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Header incorrectly encoded, left 2 bytes from size indicated 8", de.getMessage());
        }
    }
}
