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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.junit.Test;
import org.mockito.Mockito;

public class ApplicationPropertiesTypeTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeSmallSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        long currentTime = System.currentTimeMillis();

        propertiesMap.put("long-1", currentTime);
        propertiesMap.put("date-1", new Date(currentTime));
        propertiesMap.put("long-2", currentTime + 100);
        propertiesMap.put("date-2", new Date(currentTime + 100));
        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(properties);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof ApplicationProperties);

            ApplicationProperties decoded = (ApplicationProperties) result;

            assertEquals(properties.getValue().size(), decoded.getValue().size());
            assertTrue(decoded.getValue().equals(propertiesMap));

            assertEquals(currentTime, decoded.getValue().get("long-1"));
            assertEquals(new Date(currentTime), decoded.getValue().get("date-1"));
            assertEquals(currentTime + 100, decoded.getValue().get("long-2"));
            assertEquals(new Date(currentTime + 100), decoded.getValue().get("date-2"));
        }
    }

    @Test
    public void testEncodeApplicationPropertiesReservesSpaceForPayload() throws IOException {
        final int ENTRIES = 8;

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        for (int i = 0; i < ENTRIES; ++i) {
            properties.getValue().put(String.valueOf(i), i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeObject(properties);

        // Check that the Type tries to reserve space, actual encoding size not computed here.
        // Each key should also try and reserve space for the String data
        Mockito.verify(spy, Mockito.times(ENTRIES + 1)).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testDecodeApplicationProperties() throws Exception {
        Object o = doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(false, 25, 6);
        assertDecodedApplicationProperties(o);

        o = doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 28, 6);
        assertDecodedApplicationProperties(o);
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(false, 25 + 1, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map size 26 cannot be more than remaining bytes 25", de.getMessage());
        }

        try {
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 28 + 1, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map size 29 cannot be more than remaining bytes 28", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 28, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full map, so the 'remaining' checks dont foul
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 20, 22);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count 22 must be less than size (20)", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithOddCount() throws Exception {
        try {
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(false, 25, 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count 5 cannot be odd", de.getMessage());
        }

        try {
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 28, 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count 5 cannot be odd", de.getMessage());
        }
    }

    private Object doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(final boolean map32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x74); // application-properties ulong descriptor
        if (map32) {
            tmpBuf.put((byte) (byte) 0xD1); // map32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC1); // map8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }

        tmpBuf.put((byte) 0xA1); // str8
        tmpBuf.put((byte) 1); // length
        tmpBuf.put((byte) 0x33); // "3"
        tmpBuf.put((byte) 0x71); // int
        tmpBuf.putInt(3);

        tmpBuf.put((byte) 0xA1); // str8
        tmpBuf.put((byte) 1); // length
        tmpBuf.put((byte) 0x32); // "2"
        tmpBuf.put((byte) 0x71); // int
        tmpBuf.putInt(2);

        tmpBuf.put((byte) 0xA1); // str8
        tmpBuf.put((byte) 1); // length
        tmpBuf.put((byte) 0x31); // "1"
        tmpBuf.put((byte) 0x71); // int
        tmpBuf.putInt(1);

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

    private void assertDecodedApplicationProperties(Object o) {
        assertTrue(o instanceof ApplicationProperties);
        Map<String,Object> map = ((ApplicationProperties)o).getValue();
        assertTrue(map instanceof LinkedHashMap);

        assertEquals(3, map.size());
        assertEquals(Integer.valueOf(3), map.get("3"));
        assertEquals(Integer.valueOf(2), map.get("2"));
        assertEquals(Integer.valueOf(1), map.get("1"));

        Iterator<Map.Entry<String,Object>> iterator = map.entrySet().iterator();
        assertTrue(iterator.hasNext());

        Entry<String, Object> entry = iterator.next();
        assertEquals("3", entry.getKey());
        assertEquals(Integer.valueOf(3), entry.getValue());

        assertTrue(iterator.hasNext());
        entry = iterator.next();
        assertEquals("2", entry.getKey());
        assertEquals(Integer.valueOf(2), entry.getValue());

        assertTrue(iterator.hasNext());
        entry = iterator.next();
        assertEquals("1", entry.getKey());
        assertEquals(Integer.valueOf(1), entry.getValue());
    }

    @Test
    public void testDecodeOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part map, but count for full map...will try to decode count, overflow size
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(false, 25 - 8, 6);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part map, but count for full map...will try to decode count, overflow size
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 28 - 8, 6);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full map, but count for part map...will try to decode count, underflow size
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(false, 25, 4);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("ApplicationProperties incorrectly encoded, left 8 bytes from size indicated 25", de.getMessage());
        }

        try {
            // Deliberately pass size for full map, but count for part map...will try to decode count, underflow size
            doDecodeApplicationPropertiesWithSpecificSizeCountTestImpl(true, 28, 4);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("ApplicationProperties incorrectly encoded, left 8 bytes from size indicated 28", de.getMessage());
        }
    }
}
