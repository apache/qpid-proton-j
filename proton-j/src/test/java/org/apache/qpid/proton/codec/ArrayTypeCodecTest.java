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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test decoding of AMQP Array types
 */
public class ArrayTypeCodecTest extends CodecTestSupport {

    private final int LARGE_ARRAY_SIZE = 2048;
    private final int SMALL_ARRAY_SIZE = 32;

    @Test
    public void testArrayOfPrimitiveBooleanObjects() throws IOException {
        final int size = 10;

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject(buffer);
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfPrimitiveBooleanObjects() throws IOException {
        boolean[] source = new boolean[0];

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testArrayOfBooleanObjects() throws IOException {
        final int size = 10;

        Boolean[] source = new Boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testZeroSizedArrayOfBooleanObjects() throws IOException {
        Boolean[] source = new Boolean[0];

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(source.length, array.length);
    }

    @Test
    public void testDecodeSmallBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeBooleanArray() throws IOException {
        doTestDecodeBooleanArrayType(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeBooleanArrayType(int size) throws IOException {
        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());
        assertTrue(result.getClass().getComponentType().isPrimitive());

        boolean[] array = (boolean[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testDecodeSmallSymbolArray() throws IOException {
        doTestDecodeSymbolArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeSymbolArray() throws IOException {
        doTestDecodeSymbolArrayType(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeSymbolArrayType(int size) throws IOException {
        Symbol[] source = new Symbol[size];
        for (int i = 0; i < size; ++i) {
            source[i] = Symbol.valueOf("test->" + i);
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Symbol[] array = (Symbol[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @Test
    public void testDecodeSmallUUIDArray() throws IOException {
        doTestDecodeUUDIArrayType(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testDecodeLargeUUDIArray() throws IOException {
        doTestDecodeUUDIArrayType(LARGE_ARRAY_SIZE);
    }

    private void doTestDecodeUUDIArrayType(int size) throws IOException {
        UUID[] source = new UUID[size];
        for (int i = 0; i < size; ++i) {
            source[i] = UUID.randomUUID();
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        UUID[] array = (UUID[]) result;
        assertEquals(size, array.length);

        for (int i = 0; i < size; ++i) {
            assertEquals(source[i], array[i]);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testArrayOfListsOfUUIDs() throws IOException {
        ArrayList<UUID>[] source = new ArrayList[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = new ArrayList<UUID>(3);
            source[i].add(UUID.randomUUID());
            source[i].add(UUID.randomUUID());
            source[i].add(UUID.randomUUID());
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        List[] list = (List[]) result;
        assertEquals(source.length, list.length);

        for (int i = 0; i < list.length; ++i) {
            assertEquals(source[i], list[i]);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testArrayOfMApsOfStringToUUIDs() throws IOException {
        Map<String, UUID>[] source = new LinkedHashMap[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = new LinkedHashMap<String, UUID>();
            source[i].put("1", UUID.randomUUID());
            source[i].put("2", UUID.randomUUID());
            source[i].put("3", UUID.randomUUID());
        }

        encoder.writeArray(source);

        buffer.clear();

        Object result = decoder.readObject();
        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Map[] map = (Map[]) result;
        assertEquals(source.length, map.length);

        for (int i = 0; i < map.length; ++i) {
            assertEquals(source[i], map[i]);
        }
    }

    @Test
    public void testEncodeSmallBooleanArrayReservesSpaceForPayload() throws IOException {
        doTestEncodeBooleanArrayTypeReservation(SMALL_ARRAY_SIZE);
    }

    @Test
    public void testEncodeLargeBooleanArrayReservesSpaceForPayload() throws IOException {
        doTestEncodeBooleanArrayTypeReservation(LARGE_ARRAY_SIZE);
    }

    private void doTestEncodeBooleanArrayTypeReservation(int size) throws IOException {
        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeArray(source);

        // Check that the ArrayType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testEncodeDecodeByteArray100() throws Throwable {
        // byte array8 less than 128 bytes
        doEncodeDecodeByteArrayTestImpl(100);
    }

    @Test
    public void testEncodeDecodeByteArray192() throws Throwable {
        // byte array8 greater than 128 bytes
        doEncodeDecodeByteArrayTestImpl(192);
    }

    @Test
    public void testEncodeDecodeByteArray384() throws Throwable {
        // byte array32
        doEncodeDecodeByteArrayTestImpl(384);
    }

    private void doEncodeDecodeByteArrayTestImpl(int count) throws Throwable {
        byte[] source = createPayloadArrayBytes(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 254 ? 1 : 4; // less than 254 and not 256, since we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + count; // variable width for element count + byte type descriptor + number of elements
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x51); // 'byte' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.put(source[i]);
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(byte.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (byte[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static byte[] createPayloadArrayBytes(int length) {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeBooleanArray100() throws Throwable {
        // boolean array8 less than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(100);
    }

    @Test
    public void testEncodeDecodeBooleanArray192() throws Throwable {
        // boolean array8 greater than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(192);
    }

    @Test
    public void testEncodeDecodeBooleanArray384() throws Throwable {
        // boolean array32
        doEncodeDecodeBooleanArrayTestImpl(384);
    }

    private void doEncodeDecodeBooleanArrayTestImpl(int count) throws Throwable {
        boolean[] source = createPayloadArrayBooleans(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 254 ? 1 : 4; // less than 254 and not 256, since we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + count; // variable width for element count + byte type descriptor + number of elements
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x56); // 'boolean' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                byte booleanCode = (byte) (source[i] ? 0x01 : 0x00); //  0x01 true, 0x00 false.
                expectedEncodingWrapper.put(booleanCode);
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(boolean.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (boolean[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static boolean[] createPayloadArrayBooleans(int length) {
        Random rand = new Random(System.currentTimeMillis());

        boolean[] payload = new boolean[length];
        for (int i = 0; i < length; i++) {
            payload[i] = rand.nextBoolean();
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeShortArray50() throws Throwable {
        // short array8 less than 128 bytes
        doEncodeDecodeShortArrayTestImpl(50);
    }

    @Test
    public void testEncodeDecodeShortArray100() throws Throwable {
        // short array8 greater than 128 bytes
        doEncodeDecodeShortArrayTestImpl(100);
    }

    @Test
    public void testEncodeDecodeShortArray384() throws Throwable {
        // short array32
        doEncodeDecodeShortArrayTestImpl(384);
    }

    private void doEncodeDecodeShortArrayTestImpl(int count) throws Throwable {
        short[] source = createPayloadArrayShorts(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 127 ? 1 : 4; // less than 127, since each element is 2 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 2); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x61); // 'short' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putShort(source[i]);
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(short.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (short[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static short[] createPayloadArrayShorts(int length) {
        Random rand = new Random(System.currentTimeMillis());

        short[] payload = new short[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (short) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeIntArray10() throws Throwable {
        // int array8 less than 128 bytes
        doEncodeDecodeIntArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeIntArray50() throws Throwable {
        // int array8 greater than 128 bytes
        doEncodeDecodeIntArrayTestImpl(50, false);
    }

    @Test
    public void testEncodeDecodeIntArray384() throws Throwable {
        // int array32
        doEncodeDecodeIntArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeSmallIntArray100() throws Throwable {
        // small-int array8 less than 128 bytes
        doEncodeDecodeIntArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeSmallIntArray192() throws Throwable {
        // small-int array8 greater than 128 bytes
        doEncodeDecodeIntArrayTestImpl(192, true);
    }

    @Test
    public void testEncodeDecodeSmallIntArray384() throws Throwable {
        // small-int array32
        doEncodeDecodeIntArrayTestImpl(384, true);
    }

    private void doEncodeDecodeIntArrayTestImpl(int count, boolean smallInt) throws Throwable {
        int[] source = createPayloadArrayInts(count, smallInt);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth;
            int elementWidth;
            if(smallInt) {
                elementWidth = 1;
                encodingWidth = count < 254 ? 1 : 4; // less than 254 and not 256, since each element is 1 byte, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            } else {
                elementWidth = 4;
                encodingWidth = count < 63 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            }

            int arrayPayloadSize =  encodingWidth + 1 + (count * elementWidth); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            if(smallInt) {
                expectedEncodingWrapper.put((byte) 0x54); // 'small int' type descriptor code
            } else {
                expectedEncodingWrapper.put((byte) 0x71); // 'int' type descriptor code
            }

            // Write the elements
            for (int i = 0; i < count; i++) {
                int j = source[i];
                if(smallInt) {
                    // Using put as small int is 1 byte
                    assertTrue("Unexpected value: + j", j <= 127 && j >=-128);
                    expectedEncodingWrapper.put((byte) j);
                } else {
                    expectedEncodingWrapper.putInt(j);
                }
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(int.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (int[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static int[] createPayloadArrayInts(int length, boolean smallInt) {
        Random rand = new Random(System.currentTimeMillis());

        int[] payload = new int[length];
        for (int i = 0; i < length; i++) {
            if(smallInt) {
                payload[i] = 64 + 1 + rand.nextInt(9);
            } else {
                payload[i] = 128 + 1 + rand.nextInt(9);
            }
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeLongArray10() throws Throwable {
        // long array8 less than 128 bytes
        doEncodeDecodeLongArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeLongArray25() throws Throwable {
        // long array8 greater than 128 bytes
        doEncodeDecodeLongArrayTestImpl(25, false);
    }

    @Test
    public void testEncodeDecodeLongArray384() throws Throwable {
        // long array32
        doEncodeDecodeLongArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeSmallLongArray100() throws Throwable {
        // small-long array8 less than 128 bytes
        doEncodeDecodeLongArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeSmallLongArray192() throws Throwable {
        // small-long array8 greater than 128 bytes
        doEncodeDecodeLongArrayTestImpl(192, true);
    }

    @Test
    public void testEncodeDecodeSmallLongArray384() throws Throwable {
        // small-long array32
        doEncodeDecodeLongArrayTestImpl(384, true);
    }

    private void doEncodeDecodeLongArrayTestImpl(int count, boolean smallLong) throws Throwable {
        long[] source = createPayloadArrayLongs(count, smallLong);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth;
            int elementWidth;
            if(smallLong) {
                elementWidth = 1;
                encodingWidth = count < 254 ? 1 : 4; // less than 254 and not 256, since each element is 1 byte, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            } else {
                elementWidth = 8;
                encodingWidth = count < 31 ? 1 : 4; // less than 31, since each element is 8 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            }

            int arrayPayloadSize =  encodingWidth + 1 + (count * elementWidth); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            if(smallLong) {
                expectedEncodingWrapper.put((byte) 0x55); // 'small long' type descriptor code
            } else {
                expectedEncodingWrapper.put((byte) 0x81); // 'long' type descriptor code
            }

            // Write the elements
            for (int i = 0; i < count; i++) {
                long j = source[i];
                if(smallLong) {
                    // Using put as small long is 1 byte
                    assertTrue("Unexpected value: + j", j <= 127 && j >=-128);
                    expectedEncodingWrapper.put((byte) j);
                } else {
                    expectedEncodingWrapper.putLong(j);
                }
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(long.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (long[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static long[] createPayloadArrayLongs(int length, boolean smallLong) {
        Random rand = new Random(System.currentTimeMillis());

        long[] payload = new long[length];
        for (int i = 0; i < length; i++) {
            if(smallLong) {
                payload[i] = 64 + 1 + rand.nextInt(9);
            } else {
                payload[i] = 128 + 1 + rand.nextInt(9);
            }
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeFloatArray25() throws Throwable {
        // float array8 less than 128 bytes
        doEncodeDecodeFloatArrayTestImpl(25);
    }

    @Test
    public void testEncodeDecodeFloatArray50() throws Throwable {
        // float array8 greater than 128 bytes
        doEncodeDecodeFloatArrayTestImpl(50);
    }

    @Test
    public void testEncodeDecodeFloatArray384() throws Throwable {
        // float array32
        doEncodeDecodeFloatArrayTestImpl(384);
    }

    private void doEncodeDecodeFloatArrayTestImpl(int count) throws Throwable {
        float[] source = createPayloadArrayFloats(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 63 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 4); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x72); // 'float' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putFloat(source[i]);
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(float.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (float[]) decoded, 0.0F);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static float[] createPayloadArrayFloats(int length) {
        Random rand = new Random(System.currentTimeMillis());

        float[] payload = new float[length];
        for (int i = 0; i < length; i++) {
            payload[i] = 64 + 1 + rand.nextInt(9);
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeDoubleArray10() throws Throwable {
        // double array8 less than 128 bytes
        doEncodeDecodeDoubleArrayTestImpl(10);
    }

    @Test
    public void testEncodeDecodeDoubleArray25() throws Throwable {
        // double array8 greater than 128 bytes
        doEncodeDecodeDoubleArrayTestImpl(25);
    }

    @Test
    public void testEncodeDecodeDoubleArray384() throws Throwable {
        // double array32
        doEncodeDecodeDoubleArrayTestImpl(384);
    }

    private void doEncodeDecodeDoubleArrayTestImpl(int count) throws Throwable {
        double[] source = createPayloadArrayDoubles(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 31 ? 1 : 4; // less than 31, since each element is 8 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 8); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x82); // 'double' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putDouble(source[i]);
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(double.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (double[]) decoded, 0.0F);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static double[] createPayloadArrayDoubles(int length) {
        Random rand = new Random(System.currentTimeMillis());

        double[] payload = new double[length];
        for (int i = 0; i < length; i++) {
            payload[i] = 64 + 1 + rand.nextInt(9);
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeCharArray25() throws Throwable {
        // char array8 less than 128 bytes
        doEncodeDecodeCharArrayTestImpl(25);
    }

    @Test
    public void testEncodeDecodeCharArray50() throws Throwable {
        // char array8 greater than 128 bytes
        doEncodeDecodeCharArrayTestImpl(50);
    }

    @Test
    public void testEncodeDecodeCharArray384() throws Throwable {
        // char array32
        doEncodeDecodeCharArrayTestImpl(384);
    }

    private void doEncodeDecodeCharArrayTestImpl(int count) throws Throwable {
        char[] source = createPayloadArrayChars(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 63 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 4); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x73); // 'char' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putInt(source[i]); //4 byte encoding
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(char.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (char[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static char[] createPayloadArrayChars(int length) {
        Random rand = new Random(System.currentTimeMillis());

        char[] payload = new char[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (char) (64 + 1 + rand.nextInt(9));
        }

        return payload;
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray50() throws Throwable {
        // sym8 array8 less than 128 bytes
        doEncodeDecodeSmallSymbolArrayTestImpl(50);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray100() throws Throwable {
        // sym8 array8 greater than 128 bytes
        doEncodeDecodeSmallSymbolArrayTestImpl(100);
    }

    @Test
    public void testEncodeDecodeSmallSymbolArray384() throws Throwable {
        // sym8 array32
        doEncodeDecodeSmallSymbolArrayTestImpl(384);
    }

    private void doEncodeDecodeSmallSymbolArrayTestImpl(int count) throws Throwable {
        Symbol[] source = createPayloadArraySmallSymbols(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 127 ? 1 : 4; // less than 127, since each element is 2 bytes (1 length, 1 content char), but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 2); // variable width for element count + byte type descriptor + (number of elements * size[=length+content-char])
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(count < 254) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0xA3); // 'sym8' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                Symbol symbol = source[i];
                assertEquals("Unexpected length", 1, symbol.length());

                expectedEncodingWrapper.put((byte) 1); // Length
                expectedEncodingWrapper.put((byte) symbol.toString().charAt(0)); // Content
            }

            assertFalse("Should have filled expected encoding array", expectedEncodingWrapper.hasRemaining());

            // Now verify against the actual encoding of the array
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, buffer.position());

            byte[] actualEncoding = new byte[expectedEncodedArraySize];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertEquals(Symbol.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (Symbol[]) decoded);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    // Creates 1 char Symbols with chars of 0-9, for encoding as sym8
    private static Symbol[] createPayloadArraySmallSymbols(int length) {
        Random rand = new Random(System.currentTimeMillis());

        Symbol[] payload = new Symbol[length];
        for (int i = 0; i < length; i++) {
            payload[i] = Symbol.valueOf(String.valueOf(rand.nextInt(9)));
        }

        return payload;
    }
}