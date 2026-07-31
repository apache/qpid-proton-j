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
import java.util.HashMap;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.junit.Test;

/**
 * Test for decoder of AMQP Flow type.
 */
public class FlowCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeFlow() throws IOException {
        doTestDecodeFlowSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfFlows() throws IOException {
        doTestDecodeFlowSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfFlows() throws IOException {
        doTestDecodeFlowSeries(LARGE_SIZE);
    }

    private void doTestDecodeFlowSeries(int size) throws IOException {
        Flow flow = new Flow();
        flow.setNextIncomingId(UnsignedInteger.valueOf(1));
        flow.setIncomingWindow(UnsignedInteger.valueOf(2));
        flow.setNextOutgoingId(UnsignedInteger.valueOf(3));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(4));
        flow.setHandle(UnsignedInteger.valueOf(5));
        flow.setDeliveryCount(UnsignedInteger.valueOf(6));
        flow.setLinkCredit(UnsignedInteger.valueOf(7));
        flow.setAvailable(UnsignedInteger.valueOf(8));
        flow.setDrain(true);
        flow.setEcho(true);
        flow.setProperties(new HashMap<>());

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(flow);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Flow);

            Flow decoded = (Flow) result;
            assertEquals(flow.getNextIncomingId(), decoded.getNextIncomingId());
            assertEquals(flow.getIncomingWindow(), decoded.getIncomingWindow());
            assertEquals(flow.getNextOutgoingId(), decoded.getNextOutgoingId());
            assertEquals(flow.getOutgoingWindow(), decoded.getOutgoingWindow());
            assertEquals(flow.getHandle(), decoded.getHandle());
            assertEquals(flow.getDeliveryCount(), decoded.getDeliveryCount());
            assertEquals(flow.getLinkCredit(), decoded.getLinkCredit());
            assertEquals(flow.getAvailable(), decoded.getAvailable());
            assertEquals(flow.getDrain(), decoded.getDrain());
            assertEquals(flow.getEcho(), decoded.getEcho());
            assertEquals(flow.getProperties(), decoded.getProperties());
        }
    }

    @Test
    public void testDecodeFlow2() throws Exception {
        Object o = doDecodeFlowWithSpecificSizeCountTestImpl(false, 26, 5);
        assertDecodedFlow(o);

        o = doDecodeFlowWithSpecificSizeCountTestImpl(true, 29, 5);
        assertDecodedFlow(o);
    }

    private void assertDecodedFlow(Object o) {
        assertTrue(o instanceof Flow);
        Flow decoded = (Flow) o;

        assertEquals(UnsignedInteger.valueOf(4), decoded.getNextIncomingId());
        assertEquals(UnsignedInteger.valueOf(3), decoded.getIncomingWindow());
        assertEquals(UnsignedInteger.valueOf(2), decoded.getNextOutgoingId());
        assertEquals(UnsignedInteger.valueOf(1), decoded.getOutgoingWindow());
        assertEquals(UnsignedInteger.valueOf(1), decoded.getHandle());
        assertNull(decoded.getDeliveryCount());
        assertNull(decoded.getLinkCredit());
        assertNull(decoded.getAvailable());
        assertFalse(decoded.getDrain());
        assertFalse(decoded.getEcho());
        assertNull(decoded.getProperties());
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeFlowWithSpecificSizeCountTestImpl(false, 26 + 1, 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 27 cannot be more than remaining bytes 26", de.getMessage());
        }

        try {
            doDecodeFlowWithSpecificSizeCountTestImpl(true, 29 + 1, 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 30 cannot be more than remaining bytes 29", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeFlowWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeFlowWithSpecificSizeCountTestImpl(true, 29, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeFlowWithSpecificSizeCountTestImpl(false, 2, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 3 must be less than size (2)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeFlowWithSpecificSizeCountTestImpl(true, 5, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 6 must be less than size (5)", de.getMessage());
        }
    }

    private Object doDecodeFlowWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x13); // flow ulong descriptor
        if (list32) {
            tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(4); // next-incoming-id
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(3); // incoming-window
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(2); // next-outgoing-id
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(1); // outgoing-window
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(1); // handle

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
            doDecodeFlowWithSpecificSizeCountTestImpl(false, 26 - 5, 5);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeFlowWithSpecificSizeCountTestImpl(true, 29 - 5, 5);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeFlowWithSpecificSizeCountTestImpl(false, 26, 5 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Flow incorrectly encoded, left 5 bytes from size indicated 26", de.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeFlowWithSpecificSizeCountTestImpl(true, 29, 5 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Flow incorrectly encoded, left 5 bytes from size indicated 29", de.getMessage());
        }
    }
}
