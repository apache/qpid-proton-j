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
import java.util.Date;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.junit.Test;

/**
 * Test for decoder of AMQP Properties type.
 */
public class PropertiesCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodePropertiess() throws IOException {
        doTestDecodePropertiesSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);
    }

    private void doTestDecodePropertiesSeries(int size) throws IOException {
        Properties properties = new Properties();

        Date timeNow = new Date(System.currentTimeMillis());

        properties.setMessageId("ID:Message-1:1:1:0");
        properties.setUserId(new Binary(new byte[1]));
        properties.setTo("queue:work");
        properties.setSubject("help");
        properties.setReplyTo("queue:temp:me");
        properties.setContentEncoding(Symbol.valueOf("text/UTF-8"));
        properties.setContentType(Symbol.valueOf("text"));
        properties.setCorrelationId("correlation-id");
        properties.setAbsoluteExpiryTime(timeNow);
        properties.setCreationTime(timeNow);
        properties.setGroupId("group-1");
        properties.setGroupSequence(UnsignedInteger.valueOf(1));
        properties.setReplyToGroupId("group-1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(properties);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Properties);

            Properties decoded = (Properties) result;

            assertNotNull(decoded.getAbsoluteExpiryTime());
            assertEquals(timeNow, decoded.getAbsoluteExpiryTime());
            assertEquals(Symbol.valueOf("text/UTF-8"), decoded.getContentEncoding());
            assertEquals(Symbol.valueOf("text"), decoded.getContentType());
            assertEquals("correlation-id", decoded.getCorrelationId());
            assertEquals(timeNow, decoded.getCreationTime());
            assertEquals("group-1", decoded.getGroupId());
            assertEquals(UnsignedInteger.valueOf(1), decoded.getGroupSequence());
            assertEquals("ID:Message-1:1:1:0", decoded.getMessageId());
            assertEquals("queue:temp:me", decoded.getReplyTo());
            assertEquals("group-1", decoded.getReplyToGroupId());
            assertEquals("help", decoded.getSubject());
            assertEquals("queue:work", decoded.getTo());
            assertTrue(decoded.getUserId() instanceof Binary);
        }
    }

    @Test
    public void testDecodeProperties2() throws Exception {
        Object o = doDecodePropertiesWithSpecificSizeCountTestImpl(false, 8, 2);
        assertDecodedProperties(o);

        o = doDecodePropertiesWithSpecificSizeCountTestImpl(true, 11, 2);
        assertDecodedProperties(o);
    }

    private void assertDecodedProperties(Object o) {
        assertDecodedProperties(o, false);
    }
    private void assertDecodedProperties(Object o, boolean empty) {
        assertTrue(o instanceof Properties);
        Properties p = (Properties) o;

        if(empty) {
            assertNull(p.getMessageId());
            assertNull(p.getUserId());
        } else {
            assertEquals(UnsignedLong.valueOf(0), p.getMessageId());
            assertEquals(new Binary(new byte[] { 0x03, 0x02, 0x01}), p.getUserId());
        }
        assertNull(p.getTo());
        assertNull(p.getSubject());
        assertNull(p.getReplyTo());
        assertNull(p.getCorrelationId());
        assertNull(p.getContentType());
        assertNull(p.getContentEncoding());
        assertNull(p.getAbsoluteExpiryTime());
        assertNull(p.getCreationTime());
        assertNull(p.getGroupId());
        assertNull(p.getGroupSequence());
        assertNull(p.getReplyToGroupId());
    }

    @Test
    public void testDecodeEmptyProperties() throws Exception {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x73); // properties ulong descriptor
        tmpBuf.put((byte) (byte) 0x45); // list0 primitive type constructor

        tmpBuf.flip();
        int oldLimit = tmpBuf.limit();

        decoder.setByteBuffer(tmpBuf);

        Object o = decoder.readObject();

        assertDecodedProperties(o, true);

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(oldLimit, tmpBuf.position());
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodePropertiesWithSpecificSizeCountTestImpl(false, 8 + 1, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 9 cannot be more than remaining bytes 8", de.getMessage());
        }

        try {
            doDecodePropertiesWithSpecificSizeCountTestImpl(true, 11 + 1, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 12 cannot be more than remaining bytes 11", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodePropertiesWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodePropertiesWithSpecificSizeCountTestImpl(true, 2, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodePropertiesWithSpecificSizeCountTestImpl(false, 2, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 3 must be less than size (2)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodePropertiesWithSpecificSizeCountTestImpl(true, 6, 7);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 7 must be less than size (6)", de.getMessage());
        }
    }

    private Object doDecodePropertiesWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x73); // properties ulong descriptor
        if (list32) {
            tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0); // message-id ulong 0

        tmpBuf.put((byte) 0xA0); // vbin8
        tmpBuf.put((byte) 3); // length
        tmpBuf.put((byte) 0x03);
        tmpBuf.put((byte) 0x02);
        tmpBuf.put((byte) 0x01);

        tmpBuf.flip();

        int oldLimit = tmpBuf.limit();

        decoder.setByteBuffer(tmpBuf);

        final Object decoded;
        try {
            decoded = decoder.readObject();
        } finally {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    @Test
    public void testDecodeOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodePropertiesWithSpecificSizeCountTestImpl(false, 8 - 5, 2);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodePropertiesWithSpecificSizeCountTestImpl(true, 11 - 5, 2);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodePropertiesWithSpecificSizeCountTestImpl(false, 8, 2 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Properties incorrectly encoded, left 5 bytes from size indicated 8", de.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodePropertiesWithSpecificSizeCountTestImpl(true, 11, 2 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Properties incorrectly encoded, left 5 bytes from size indicated 11", de.getMessage());
        }
    }
}
