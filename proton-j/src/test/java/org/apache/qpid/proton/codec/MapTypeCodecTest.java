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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.mockito.Mockito;

public class MapTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeSmallSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeMapSeries(int size) throws IOException {

        String myBoolKey = "myBool";
        boolean myBool = true;
        String myByteKey = "myByte";
        byte myByte = 4;
        String myBytesKey = "myBytes";
        byte[] myBytes = myBytesKey.getBytes();
        String myCharKey = "myChar";
        char myChar = 'd';
        String myDoubleKey = "myDouble";
        double myDouble = 1234567890123456789.1234;
        String myFloatKey = "myFloat";
        float myFloat = 1.1F;
        String myIntKey = "myInt";
        int myInt = Integer.MAX_VALUE;
        String myLongKey = "myLong";
        long myLong = Long.MAX_VALUE;
        String myShortKey = "myShort";
        short myShort = 25;
        String myStringKey = "myString";
        String myString = myStringKey;

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put(myBoolKey, myBool);
        map.put(myByteKey, myByte);
        map.put(myBytesKey, new Binary(myBytes));
        map.put(myCharKey, myChar);
        map.put(myDoubleKey, myDouble);
        map.put(myFloatKey, myFloat);
        map.put(myIntKey, myInt);
        map.put(myLongKey, myLong);
        map.put(myShortKey, myShort);
        map.put(myStringKey, myString);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(map);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Map);

            Map<String, Object> resultMap = (Map<String, Object>) result;

            assertEquals(map.size(), resultMap.size());
        }
    }

    @Test
    public void testEncodeSmallMapReservesSpaceForPayload() throws IOException {
        doTestEncodeMapTypeReservation(SMALL_SIZE);
    }

    @Test
    public void doTestEncodeMapTypeReservation() throws IOException {
        doTestEncodeMapTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeMapTypeReservation(int size) throws IOException {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < size; ++i) {
            map.put(i, i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeMap(map);

        // Check that the MapType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testEncodeMapWithUnknownEntryValueType() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("unknown", new MyUnknownTestType());

        doTestEncodeMapWithUnknownEntryValueTypeTestImpl(map);
    }

    @Test
    public void testEncodeSubMapWithUnknownEntryValueType() throws Exception {
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("unknown", new MyUnknownTestType());

        Map<String, Object> map = new HashMap<>();
        map.put("submap", subMap);

        doTestEncodeMapWithUnknownEntryValueTypeTestImpl(map);
    }

    private void doTestEncodeMapWithUnknownEntryValueTypeTestImpl(Map<String, Object> map) {
        try {
            encoder.writeMap(map);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("No encoding is known for map entry value of type: org.apache.qpid.proton.codec.MyUnknownTestType"));
        }
    }

    @Test
    public void testEncodeMapWithUnknownEntryKeyType() throws Exception {
        Map<Object, String> map = new HashMap<>();
        map.put(new MyUnknownTestType(), "unknown");

        doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(map);
    }

    @Test
    public void testEncodeSubMapWithUnknownEntryKeyType() throws Exception {
        Map<Object, String> subMap = new HashMap<>();
        subMap.put(new MyUnknownTestType(), "unknown");

        Map<String, Object> map = new HashMap<>();
        map.put("submap", subMap);

        doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(map);
    }

    private void doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(Map<?, ?> map) {
        try {
            encoder.writeMap(map);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("No encoding is known for map entry key of type: org.apache.qpid.proton.codec.MyUnknownTestType"));
        }
    }

    @Test
    public void testDecodeMapOfInteger() throws Exception {
        Object o = doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(false, 25, 6);
        assertDecodedMapOfInteger(o);

        o = doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28, 6);
        assertDecodedMapOfInteger(o);
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(false, 25 + 1, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map size 26 cannot be more than remaining bytes 25", de.getMessage());
        }

        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28 + 1, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map size 29 cannot be more than remaining bytes 28", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full map, so the 'remaining' checks dont foul
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 20, 22);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count 22 must be less than size (20)", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithOddCount() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(false, 25, 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count 5 cannot be odd", de.getMessage());
        }

        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28, 5);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map element count 5 cannot be odd", de.getMessage());
        }
    }

    private Object doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(final boolean map32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
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

    private void assertDecodedMapOfInteger(Object o) {
        assertTrue(o instanceof Map);
        assertTrue(o instanceof LinkedHashMap);
        @SuppressWarnings("unchecked")
        Map<Object,Object> map = (Map<Object, Object>) o;

        assertEquals(3, map.size());
        assertEquals(Integer.valueOf(3), map.get("3"));
        assertEquals(Integer.valueOf(2), map.get("2"));
        assertEquals(Integer.valueOf(1), map.get("1"));

        Iterator<Map.Entry<Object,Object>> iterator = map.entrySet().iterator();
        assertTrue(iterator.hasNext());

        Entry<Object, Object> entry = iterator.next();
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
    public void testDecodeMapOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part map, but count for full map...will try to decode count, overflow
            doDecodeUnderflowOverflowTestImpl(false, 11, 4);
            fail("Expected exception to be thrown");
        } catch (IndexOutOfBoundsException ioobe) {
            // Expected
        }

        try {
            // Deliberately pass size for part map, but count for full map...will try to decode count, overflow
            doDecodeUnderflowOverflowTestImpl(true, 14, 4);
            fail("Expected exception to be thrown");
        } catch (IndexOutOfBoundsException ioobe) {
            // Expected
        }
    }

    @Test
    public void testDecodeMapUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full map, but count for part map...will try to decode count, underflow
            doDecodeUnderflowOverflowTestImpl(false, 21, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map incorrectly encoded, left 10 bytes from size indicated 21", de.getMessage());
        }

        try {
            // Deliberately pass size for full map, but count for part map...will try to decode count, underflow
            doDecodeUnderflowOverflowTestImpl(true, 24, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Map incorrectly encoded, left 10 bytes from size indicated 24", de.getMessage());
        }
    }

    private Object doDecodeUnderflowOverflowTestImpl(final boolean map32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        if (map32) {
            tmpBuf.put((byte) (byte) 0xD1); // map32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC1); // map8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
        tmpBuf.put((byte) 0x71); // int
        tmpBuf.putInt(4);
        tmpBuf.put((byte) 0x71); // int
        tmpBuf.putInt(3);
        tmpBuf.put((byte) 0x71); // int
        tmpBuf.putInt(2);
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

        return decoded;
    }

    @Test
    public void testDecodingOfDeeplyNestedMapOfMaps() throws Exception {
        final int depthLimit = 10;
        final Map<String, Object> toEncode = new HashMap<>();

        Map<String, Object> current = toEncode;

        // Encodes one more than the max depth value set
        for (int i = 1; i < depthLimit + 1; ++i) {
            final Map<String, Object> next = new HashMap<>();

            current.put(String.valueOf(i), next);
            current = next;
        }

        encoder.writeMap(toEncode);
        decoder.setMaxDecodeDepth(depthLimit);
        buffer.flip();

        try {
            decoder.readMap();
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 10", de.getMessage());
            //expected
        }

        // Encode up to the limit instead which should work
        toEncode.clear();
        current = toEncode;

        for (int i = 1; i < depthLimit; ++i) {
            final Map<String, Object> next = new HashMap<>();

            current.put(String.valueOf(i), next);
            current = next;
        }

        buffer.clear();
        encoder.writeMap(toEncode);
        buffer.flip();

        assertTrue(decoder.readMap() instanceof Map);
    }

    @Test
    public void testDecodingOfDeeplyNestedMapOfMap32() throws Exception {
        doDecodingOfDeeplyNestedMapOfMapTestImpl(true, 10);
    }

    @Test
    public void testDecodingOfDeeplyNestedMapOfMap8() throws Exception {
        doDecodingOfDeeplyNestedMapOfMapTestImpl(false, 10);
    }

    private void doDecodingOfDeeplyNestedMapOfMapTestImpl(final boolean map32, final int depth) {
        ByteBuffer tmpBuf = buildNestedMapsBuffer(depth, map32);
        decoder.setByteBuffer(tmpBuf);

        decoder.setMaxDecodeDepth(depth - 1);

        try {
            decoder.readMap();
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: " + (depth - 1), de.getMessage());
            //expected
        }

        decoder.setMaxDecodeDepth(depth);
        tmpBuf.clear();

        assertTrue(decoder.readMap() instanceof Map);
    }

    private ByteBuffer buildNestedMapsBuffer(final int levels, boolean map32)
    {
        final int encodingWith = map32 ? 4 : 1;
        final int totalEncodingSize = ((1 + (encodingWith * 2) + 1) * levels) - 1 ; // Number of bytes for size + count + 1 byte for map type plus 1 byte for key, times number of maps
                                                                                   // then -1 (as final map is empty so no key, but still has type + size + count)

        ByteBuffer tmpBuf = ByteBuffer.allocate(totalEncodingSize);

        int currentMapSize = totalEncodingSize - encodingWith - 1; //(doesnt include its own type code and size itself)

        if(!map32) {
            assertTrue("Cant encode map8 of this depth within 255 byte map size limit", currentMapSize < 256);
        }

        final byte mapTypeCode = map32 ? EncodingCodes.MAP32 : EncodingCodes.MAP8;
        final int individualMapBytes = 1 + (encodingWith * 2) + 1; // type code, then size, then count, then key for all but last map

        for (int i = 1; i <= levels; i++)
        {
            // Type code
            tmpBuf.put(mapTypeCode);

            // Size
            if(map32) {
                tmpBuf.putInt(currentMapSize);
            } else {
                tmpBuf.put((byte) currentMapSize);
            }

            if(i < levels) {
                // Count (2 for all but last, key + the nested map value coming next)
                if(map32) {
                    tmpBuf.putInt(2);
                } else {
                    tmpBuf.put((byte) 2);
                }

                // Write a key, 1 byte, use ulong0 for ease
                tmpBuf.put((byte) EncodingCodes.ULONG0);
                // Value will be written on next iteration

                // Update encoded map size for next iteration
                currentMapSize -= individualMapBytes;
            } else {
                // Count (0 for the final empty map)
                if(map32) {
                    tmpBuf.putInt(0);
                } else {
                    tmpBuf.put((byte) 0);
                }
            }
        }

        assertEquals(encodingWith, currentMapSize); // last size is just the count of the empty map

        // Check we filled the buffer
        assertEquals(0, tmpBuf.remaining());

        tmpBuf.flip();

        return tmpBuf;
    }
}
