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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for the Proton List encoder / decoder
 */
public class ListTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeList() throws IOException {
        doTestDecodeListSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfLists() throws IOException {
        doTestDecodeListSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfLists() throws IOException {
        doTestDecodeListSeries(LARGE_SIZE);
    }

    @Test
    public void testDecodeSmallSeriesOfSymbolLists() throws IOException {
        doTestDecodeSymbolListSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfSymbolLists() throws IOException {
        doTestDecodeSymbolListSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeSymbolListSeries(int size) throws IOException {
        List<Object> list = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            list.add(Symbol.valueOf(String.valueOf(i)));
        }

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(list);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof List);

            List<Object> resultList = (List<Object>) result;

            assertEquals(list.size(), resultList.size());
        }
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeListSeries(int size) throws IOException {
        List<Object> list = new ArrayList<>();

        Date timeNow = new Date(System.currentTimeMillis());

        list.add("ID:Message-1:1:1:0");
        list.add(new Binary(new byte[1]));
        list.add("queue:work");
        list.add(Symbol.valueOf("text/UTF-8"));
        list.add(Symbol.valueOf("text"));
        list.add(timeNow);
        list.add(UnsignedInteger.valueOf(1));
        list.add(UUID.randomUUID());

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(list);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof List);

            List<Object> resultList = (List<Object>) result;

            assertEquals(list.size(), resultList.size());
        }
    }

    @Test
    public void testEncodeSmallListReservesSpaceForPayload() throws IOException {
        doTestEncodeListTypeReservation(SMALL_SIZE);
    }

    @Test
    public void testEncodeLargeListReservesSpaceForPayload() throws IOException {
        doTestEncodeListTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeListTypeReservation(int size) throws IOException {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            list.add(i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeList(list);

        // Check that the ListType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testEncodeListWithUnknownEntryType() throws Exception {
        List<Object> list = new ArrayList<>();
        list.add(new MyUnknownTestType());

        doTestEncodeListWithUnknownEntryTypeTestImpl(list);
    }

    @Test
    public void testEncodeSubListWithUnknownEntryType() throws Exception {
        List<Object> subList = new ArrayList<>();
        subList.add(new MyUnknownTestType());

        List<Object> list = new ArrayList<>();
        list.add(subList);

        doTestEncodeListWithUnknownEntryTypeTestImpl(list);
    }

    private void doTestEncodeListWithUnknownEntryTypeTestImpl(List<Object> list) {
        try {
            encoder.writeList(list);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("No encoding defined for type: class org.apache.qpid.proton.codec.MyUnknownTestType"));
        }
    }

    @Test
    public void testDecodeIntegerList() throws Exception {
        Object o = doDecodeIntegerListWithSpecificSizeCountTestImpl(false, 16, 3);
        assertDecodedIntegerList(o);

        o = doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 19, 3);
        assertDecodedIntegerList(o);
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeIntegerListWithSpecificSizeCountTestImpl(false, 17, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 17 cannot be more than remaining bytes 16", de.getMessage());
        }

        try {
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 20, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 20 cannot be more than remaining bytes 19", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 19, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeIntegerListWithSpecificSizeCountTestImpl(false, 11, 12);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 12 must be less than size (11)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 13, 14);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 14 must be less than size (13)", de.getMessage());
        }
    }

    private Object doDecodeIntegerListWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        if (list32) {
            tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
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
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void assertDecodedIntegerList(Object o) {
        assertTrue(o instanceof List<?>);
        List<?> list = (List<?>) o;

        assertEquals(3, list.size());
        assertEquals(Integer.valueOf(3), list.get(0));
        assertEquals(Integer.valueOf(2), list.get(1));
        assertEquals(Integer.valueOf(1), list.get(2));
    }

    @Test
    public void testDecodeListOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow
            doDecodeUnderflowOverflowTestImpl(false, 11, 3);
            fail("Expected exception to be thrown");
        } catch (IndexOutOfBoundsException ioobe) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow
            doDecodeUnderflowOverflowTestImpl(true, 14, 3);
            fail("Expected exception to be thrown");
        } catch (IndexOutOfBoundsException ioobe) {
            // Expected
        }
    }

    @Test
    public void testDecodeListUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow
            doDecodeUnderflowOverflowTestImpl(false, 16, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List incorrectly encoded, left 5 bytes from size indicated 16", de.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow
            doDecodeUnderflowOverflowTestImpl(true, 19, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List incorrectly encoded, left 5 bytes from size indicated 19", de.getMessage());
        }
    }

    private Object doDecodeUnderflowOverflowTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        if (list32) {
            tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
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
    public void testDecodingOfDeeplyNestedListOfLists() throws Exception {
        final int depthLimit = 10;
        final List<Object> toEncode = new ArrayList<>();

        List<Object> current = toEncode;

        // Encodes one more than the max depth value set
        for (int i = 1; i < depthLimit + 1; ++i) {
            final List<Object> next = new ArrayList<>();

            current.add(next);
            current = next;
        }

        encoder.writeList(toEncode);
        buffer.flip();
        decoder.setMaxDecodeDepth(depthLimit);

        try {
            decoder.readList();
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 10", de.getMessage());
            //expected
        }

        // Encode up to the limit instead which should work
        toEncode.clear();
        current = toEncode;

        for (int i = 1; i < depthLimit; ++i) {
            final List<Object> next = new ArrayList<>();

            current.add(next);
            current = next;
        }

        buffer.clear();
        encoder.writeList(toEncode);
        buffer.flip();

        assertTrue(decoder.readList() instanceof List);
    }

    @Test
    public void testDecodingOfDeeplyNestedListOfList32andList0() throws Exception {
        doDecodingOfDeeplyNestedListOfListandList0TestImpl(true, 10, false);
    }

    @Test
    public void testDecodingOfDeeplyNestedListOfList32andList0WithDefault() throws Exception {
        doDecodingOfDeeplyNestedListOfListandList0TestImpl(true, 33, true);
    }

    @Test
    public void testDecodingOfDeeplyNestedListOfList8andList0() throws Exception {
        doDecodingOfDeeplyNestedListOfListandList0TestImpl(false, 10, false);
    }

    @Test
    public void testDecodingOfDeeplyNestedListOfList8andList0WithDefault() throws Exception {
        doDecodingOfDeeplyNestedListOfListandList0TestImpl(false, 33, true);
    }

    private void doDecodingOfDeeplyNestedListOfListandList0TestImpl(final boolean list32, final int depth, boolean useDefaultLimit) {
        ByteBuffer tmpBuf = buildNestedListsBuffer(depth, list32);
        decoder.setByteBuffer(tmpBuf);

        if (!useDefaultLimit) {
            decoder.setMaxDecodeDepth(depth - 1);
        }

        try {
            decoder.readList();
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: " + (depth - 1), de.getMessage());
            //expected
        }

        decoder.setMaxDecodeDepth(depth);
        tmpBuf.clear();

        assertTrue(decoder.readList() instanceof List);
    }

    private ByteBuffer buildNestedListsBuffer(final int levels, boolean list32)
    {
        final int encodingWith = list32 ? 4 : 1;
        final int count = levels - 1;
        final int totalEncodingSize = ((1 + (encodingWith * 2)) * count) + 1; // Number of bytes for size + count + 1 byte for list type, times <number of lists minus 1>
                                                                              // (final list is a list0, has no size/count), plus 1 for final list0 constructor

        ByteBuffer tmpBuf = ByteBuffer.allocate(totalEncodingSize);

        int currentListSize = totalEncodingSize - encodingWith - 1; // Size does not include its own type code and size itself

        if(!list32) {
            assertTrue("Cant encode list8 of this depth within 255 byte list size limit", currentListSize < 256);
        }

        final byte listTypeCode = list32 ? EncodingCodes.LIST32 : EncodingCodes.LIST8;
        final int individualListBytes = 1 + (encodingWith * 2); // type code, then size, then count

        for (int i = 1; i <= count; i++)
        {
            // Type code
            tmpBuf.put(listTypeCode);

            // Size
            if(list32) {
                tmpBuf.putInt(currentListSize);
            } else {
                tmpBuf.put((byte) currentListSize);
            }

            // Count (always 1)
            if(list32) {
                tmpBuf.putInt(1);
            } else {
                tmpBuf.put((byte) 1);
            }

            // Update encoded list size for next iteration (if any)
            if(i < count) {
                currentListSize -= individualListBytes;
            }
        }

        assertEquals(encodingWith + 1, currentListSize); // count of prior list + the final list0 type constructor below

        tmpBuf.put(EncodingCodes.LIST0);

        // Check we filled the buffer
        assertEquals(0, tmpBuf.remaining());

        tmpBuf.flip();

        return tmpBuf;
    }
}
