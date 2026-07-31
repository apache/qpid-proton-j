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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
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
    public void testEncodeDecodeByteArray253() throws Throwable {
        doEncodeDecodeByteArrayTestImpl(253);
    }

    @Test
    public void testEncodeDecodeByteArray254() throws Throwable {
        doEncodeDecodeByteArrayTestImpl(254);
    }

    @Test
    public void testEncodeDecodeByteArray384() throws Throwable {
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
            if(encodingWidth == 1) {
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
    public void testEncodeDecodeBooleanArray0() throws Throwable {
        // boolean array8 size 0
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(0));
    }

    @Test
    public void testEncodeDecodeBooleanArray100() throws Throwable {
        // boolean array8 less than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(100));
    }

    @Test
    public void testEncodeDecodeBooleanArray192() throws Throwable {
        // boolean array8 greater than 128 bytes
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(192));
    }

    @Test
    public void testEncodeDecodeBooleanArray253() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(253));
    }

    @Test
    public void testEncodeDecodeBooleanArray254() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(254));
    }

    @Test
    public void testEncodeDecodeBooleanArray255() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(255));
    }

    @Test
    public void testEncodeDecodeBooleanArray384() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBooleans(384));
    }

    @Test
    public void testEncodeDecodeBooleanArrayTrue100() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(true, 100));
    }

    @Test
    public void testEncodeDecodeBooleanArrayTrue192() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(true, 192));
    }

    @Test
    public void testEncodeDecodeBooleanArrayTrue253() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(true, 253));
    }

    @Test
    public void testEncodeDecodeBooleanArrayTrue254() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(true, 254));
    }

    @Test
    public void testEncodeDecodeBooleanArrayTrue255() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(true, 255));
    }

    @Test
    public void testEncodeDecodeBooleanArrayTrue384() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(true, 384));
    }

    @Test
    public void testEncodeDecodeBooleanArrayFalse100() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(false, 100));
    }

    @Test
    public void testEncodeDecodeBooleanArrayFalse192() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(false, 192));
    }

    @Test
    public void testEncodeDecodeBooleanArrayFalse253() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(false, 253));
    }

    @Test
    public void testEncodeDecodeBooleanArrayFalse254() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(false, 254));
    }

    @Test
    public void testEncodeDecodeBooleanArrayFalse255() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(false, 255));
    }

    @Test
    public void testEncodeDecodeBooleanArrayFalse384() throws Throwable {
        doEncodeDecodeBooleanArrayTestImpl(createPayloadArrayBoolean(false, 384));
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse0() throws Throwable {
        Object decoded = doDecodeBooleanArrayTrueFalseOnlyTestImpl(true, 0, false);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(boolean.class, decoded.getClass().getComponentType());
        assertArrayEquals(new boolean[0], (boolean[]) decoded);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse0Array32() throws Throwable {
        Object decoded = doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 0, false);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(boolean.class, decoded.getClass().getComponentType());
        assertArrayEquals(new boolean[0], (boolean[]) decoded);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse255() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(true, 255, false);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse255WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(true, 255, false);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse255Array32() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 255, false);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse255Array32WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(false, 255, false);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse256() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 256, false);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse256WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(false, 256, false);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse384() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 384, false);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue0() throws Throwable {
        Object decoded = doDecodeBooleanArrayTrueFalseOnlyTestImpl(true, 0, true);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(boolean.class, decoded.getClass().getComponentType());
        assertArrayEquals(new boolean[0], (boolean[]) decoded);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue0Array32() throws Throwable {
        Object decoded = doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 0, true);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(boolean.class, decoded.getClass().getComponentType());
        assertArrayEquals(new boolean[0], (boolean[]) decoded);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue10() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(true, 10, true);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue10WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(true, 10, true);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue10Array32() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 10, true);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue10Array32WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(false, 10, true);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue255() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(true, 255, true);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue255WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(true, 255, true);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue255Array32() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 255, true);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue255Array32WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(false, 255, true);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue256() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 256, true);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue256WhenAllowed() throws Throwable {
        doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(false, 256, true);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue384() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(false, 384, true);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    private void doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(boolean array8, final int count, final boolean type) throws Throwable {
        decoder.setZeroWidthArrayElementLimit(count);
        Object decoded = doDecodeBooleanArrayTrueFalseOnlyTestImpl(array8, count, type);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(boolean.class, decoded.getClass().getComponentType());

        final boolean[] expected = new boolean[count];
        Arrays.fill(expected, type);

        assertArrayEquals(expected, (boolean[]) decoded);
    }

    private Object doDecodeBooleanArrayTrueFalseOnlyTestImpl(boolean array8, final int count, final boolean type) throws Exception {
        if (array8) {
            assertTrue("Cant encode boolean-true/boolean-false array8 with count > 255", count <= 255);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1; // variable width for element count + byte type descriptor
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload
        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }
        // Write the type descriptor
        buffer.put(type ? (byte) EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        return decoder.readObject();
    }

    private void doEncodeDecodeBooleanArrayTestImpl(final boolean[] source) throws Throwable {
        final int count = source.length;

        try {
            int encodingWidth = source.length < 254 ? 1 : 4; // less than 254 and not 256, since we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + count; // variable width for element count + byte type descriptor + number of elements
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
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

            // Now produce actual encoding of the array for later comparison
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);

            byte[] actualEncoding = new byte[buffer.position()];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertTrue(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(boolean.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (boolean[]) decoded);

            // Now verify expected encoding against the actual encoding of the array
            assertEquals("Unexpected encoded payload length", expectedEncodedArraySize, actualEncoding.length);
            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);
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

    private static boolean[] createPayloadArrayBoolean(final boolean values, final int length) {
        boolean[] payload = new boolean[length];
        for (int i = 0; i < length; i++) {
            payload[i] = values;
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
    public void testEncodeDecodeShortArray126() throws Throwable {
        doEncodeDecodeShortArrayTestImpl(126);
    }

    @Test
    public void testEncodeDecodeShortArray127() throws Throwable {
        doEncodeDecodeShortArrayTestImpl(127);
    }

    @Test
    public void testEncodeDecodeShortArray384() throws Throwable {
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
            if(encodingWidth == 1) {
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
    public void testEncodeDecodeIntArray63() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(63, false);
    }

    @Test
    public void testEncodeDecodeIntArray64() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(64, false);
    }

    @Test
    public void testEncodeDecodeIntArray384() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeSmallIntArray63() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(63, true);
    }

    @Test
    public void testEncodeDecodeSmallIntArray64() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(64, true);
    }

    @Test
    public void testEncodeDecodeSmallIntArray100() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeSmallIntArray192() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(192, true);
    }

    @Test
    public void testEncodeDecodeSmallIntArray384() throws Throwable {
        doEncodeDecodeIntArrayTestImpl(384, true);
    }

    @Test
    public void testDecodeSmallIntArray253() throws Throwable {
        final int count = 253;
        doDecodeSmallLongArrayTestImpl(true, count, createPayloadArrayInts(count, true));
        buffer.clear();
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayInts(count, true));
    }

    @Test
    public void testDecodeSmallIntArray254() throws Throwable {
        final int count = 254;
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayInts(count, true));
    }

    @Test
    public void testDecodeSmallIntArray255() throws Throwable {
        final int count = 255;
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayInts(count, true));
    }

    @Test
    public void testDecodeSmallIntArray384() throws Throwable {
        final int count = 384;
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayInts(count, true));
    }

    private void doDecodeSmallLongArrayTestImpl(final boolean array8, final int count, int[] smallInts) throws Exception {
        if (array8) {
            assertTrue("Cant encode small-int array8 with count " + count, count <= 253);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1 + (count * 1); // variable width for element count + byte type descriptor + elements
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload

        if (array8) {
            assertTrue("Cant encode array8 with arrayPayloadSize " + count, arrayPayloadSize < 256);
        }

        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x54); // 'small-int' type descriptor code
        for(int element : smallInts) {
            assertTrue(element <= 127 && element >= -128);

            buffer.put((byte) element);
        }

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        Object o = decoder.readObject();

        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertEquals(int.class, o.getClass().getComponentType());

        int[] decoded = (int[]) o;
        assertArrayEquals(smallInts, decoded);
    }

    private void doEncodeDecodeIntArrayTestImpl(int count, boolean smallInt) throws Throwable {
        int[] source = createPayloadArrayInts(count, smallInt);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int elementWidth = 4;
            int encodingWidth = count < 64 ? 1 : 4; // less than 63, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.

            int arrayPayloadSize =  encodingWidth + 1 + (count * elementWidth); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x71); // 'int' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                int j = source[i];
                expectedEncodingWrapper.putInt(j);
            }

            assertEquals("Should have filled expected encoding array", 0, expectedEncodingWrapper.remaining());

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
        doEncodeDecodeLongArrayTestImpl(10, false);
    }

    @Test
    public void testEncodeDecodeLongArray31() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(31, false);
    }

    @Test
    public void testEncodeDecodeLongArray32() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(32, false);
    }

    @Test
    public void testEncodeDecodeLongArray384() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(384, false);
    }

    @Test
    public void testEncodeDecodeSmallLongArray31() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(30, true);
    }

    @Test
    public void testEncodeDecodeSmallLongArray32() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(30, true);
    }

    @Test
    public void testEncodeDecodeSmallLongArray100() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(100, true);
    }

    @Test
    public void testEncodeDecodeSmallLongArray192() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(192, true);
    }

    @Test
    public void testEncodeDecodeSmallLongArray384() throws Throwable {
        doEncodeDecodeLongArrayTestImpl(384, true);
    }

    @Test
    public void testDecodeSmallLongArray253() throws Throwable {
        final int count = 253;
        doDecodeSmallLongArrayTestImpl(true, count, createPayloadArrayLongs(count, true));
        buffer.clear();
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayLongs(count, true));
    }

    @Test
    public void testDecodeSmallLongArray254() throws Throwable {
        final int count = 254;
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayLongs(count, true));
    }

    @Test
    public void testDecodeSmallLongArray255() throws Throwable {
        final int count = 255;
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayLongs(count, true));
    }

    @Test
    public void testDecodeSmallLongArray384() throws Throwable {
        final int count = 384;
        doDecodeSmallLongArrayTestImpl(false, count, createPayloadArrayLongs(count, true));
    }

    private void doDecodeSmallLongArrayTestImpl(final boolean array8, final int count, long[] smallLongs) throws Exception {
        if (array8) {
            assertTrue("Cant encode small-long array8 with count " + count, count <= 253);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1 + (count * 1); // variable width for element count + byte type descriptor + elements
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload

        if (array8) {
            assertTrue("Cant encode array8 with arrayPayloadSize " + count, arrayPayloadSize < 256);
        }

        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x55); // 'small-long' type descriptor code
        for(long element : smallLongs) {
            assertTrue(element <= 127 && element >= -128);

            buffer.put((byte) element);
        }

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        Object o = decoder.readObject();

        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertEquals(long.class, o.getClass().getComponentType());

        long[] decoded = (long[]) o;
        assertArrayEquals(smallLongs, decoded);
    }

    private void doEncodeDecodeLongArrayTestImpl(int count, boolean smallLong) throws Throwable {
        long[] source = createPayloadArrayLongs(count, smallLong);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int elementWidth = 8;
            int encodingWidth = count < 32 ? 1 : 4; // less than 32, since each element is 8 bytes, but we also need 1 for element count, and (in this case) 1 byte for primitive element type constructor.

            int arrayPayloadSize =  encodingWidth + 1 + (count * elementWidth); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x81); // 'long' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putLong(source[i]);
            }

            assertEquals("Should have filled expected encoding array", 0, expectedEncodingWrapper.remaining());

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
    public void testEncodeDecodeFloatArray63() throws Throwable {
        doEncodeDecodeFloatArrayTestImpl(63);
    }

    @Test
    public void testEncodeDecodeFloatArray64() throws Throwable {
        doEncodeDecodeFloatArrayTestImpl(64);
    }

    @Test
    public void testEncodeDecodeFloatArray384() throws Throwable {
        doEncodeDecodeFloatArrayTestImpl(384);
    }

    private void doEncodeDecodeFloatArrayTestImpl(int count) throws Throwable {
        float[] source = createPayloadArrayFloats(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 64 ? 1 : 4; // less than 64, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 4); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
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
    public void testEncodeDecodeDoubleArray31() throws Throwable {
        doEncodeDecodeDoubleArrayTestImpl(31);
    }

    @Test
    public void testEncodeDecodeDoubleArray32() throws Throwable {
        doEncodeDecodeDoubleArrayTestImpl(32);
    }

    @Test
    public void testEncodeDecodeDoubleArray384() throws Throwable {
        doEncodeDecodeDoubleArrayTestImpl(384);
    }

    private void doEncodeDecodeDoubleArrayTestImpl(int count) throws Throwable {
        double[] source = createPayloadArrayDoubles(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 32 ? 1 : 4; // less than 32, since each element is 8 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 8); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
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
    public void testEncodeDecodeCharArray63() throws Throwable {
        doEncodeDecodeCharArrayTestImpl(63);
    }

    @Test
    public void testEncodeDecodeCharArray64() throws Throwable {
        doEncodeDecodeCharArrayTestImpl(64);
    }

    @Test
    public void testEncodeDecodeCharArray384() throws Throwable {
        doEncodeDecodeCharArrayTestImpl(384);
    }

    private void doEncodeDecodeCharArrayTestImpl(int count) throws Throwable {
        char[] source = createPayloadArrayChars(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 64 ? 1 : 4; // less than 64, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 4); // variable width for element count + byte type descriptor + (number of elements * size)
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
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
    public void testEncodeDecodeSymbolArray50() throws Throwable {
        doEncodeDecodeSymbolArrayTestImpl(50);
    }

    @Test
    public void testEncodeDecodeSymbolArray100() throws Throwable {
        doEncodeDecodeSymbolArrayTestImpl(100);
    }

    @Test
    public void testEncodeDecodeSymbolArray384() throws Throwable {
        doEncodeDecodeSymbolArrayTestImpl(384);
    }

    @Test
    public void testDecodeSmallSymbolArray126() throws Throwable {
        //Using 126 with single-char symbol since element length bytes will use up the array size
        final int count = 126;
        doDecodeSmallSymbolArrayTestImpl(true, count, Symbol.valueOf("a"));
        buffer.clear();
        doDecodeSmallSymbolArrayTestImpl(false, count, Symbol.valueOf("a"));
    }

    @Test
    public void testDecodeSmallSymbolArray253() throws Throwable {
        //Using 253 with empty symbol since element length bytes will use up the array size
        final int count = 253;
        doDecodeSmallSymbolArrayTestImpl(true, count, Symbol.valueOf(""));
        buffer.clear();
        doDecodeSmallSymbolArrayTestImpl(false, count, Symbol.valueOf(""));
    }

    @Test
    public void testDecodeSmallSymbolArray254() throws Throwable {
        final int count = 254;
        doDecodeSmallSymbolArrayTestImpl(false, count, Symbol.valueOf("a"));
    }

    @Test
    public void testDecodeSmallSymbolArray255() throws Throwable {
        final int count = 255;
        doDecodeSmallSymbolArrayTestImpl(false, count, Symbol.valueOf("a"));
    }

    @Test
    public void testDecodeSmallSymbolArray384() throws Throwable {
        final int count = 384;
        doDecodeSmallSymbolArrayTestImpl(false, count, Symbol.valueOf("a"));
    }

    private void doDecodeSmallSymbolArrayTestImpl(final boolean array8, final int count, Symbol smallSymbol) throws Exception {
        if (array8) {
            assertTrue("Cant encode sym8 array8 with count " + count, count <= 253);
            assertTrue("Individual sym8 in array8 cant exceed 253 chars", smallSymbol.length() <= 253);
        } else {
            assertTrue("Individual sym8 in array32 cant exceed 255 chars", smallSymbol.length() <= 255);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1 + (count * (1 + smallSymbol.length())); // variable width for element count + byte type descriptor + (elements = count * (length-byte + characters))
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload

        if (array8) {
            assertTrue("Cant encode array8 with arrayPayloadSize " + arrayPayloadSize, arrayPayloadSize <= 256);
        }

        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0xA3); // 'sym8' type descriptor code
        for (int i = 0; i < count; i++) {
            buffer.put((byte) smallSymbol.length());

            buffer.put(smallSymbol.toString().getBytes(StandardCharsets.US_ASCII));
        }

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        Object o = decoder.readObject();

        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertEquals(Symbol.class, o.getClass().getComponentType());

        Symbol[] decoded = (Symbol[]) o;

        Symbol[] expected = new Symbol[count];
        for(int i = 0; i < count; i++) {
            expected[i] = smallSymbol;
        }

        assertArrayEquals(expected, decoded);
    }

    private void doEncodeDecodeSymbolArrayTestImpl(int count) throws Throwable {
        Symbol[] source = createPayloadArraySmallSymbols(count);

        try {
            assertEquals("Unexpected source array length", count, source.length);

            int encodingWidth = count < 51 ? 1 : 4; // less than 51, since each element is 5 bytes (4 length, 1 content char), but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            int arrayPayloadSize =  encodingWidth + 1 + (count * 5); // variable width for element count + byte type descriptor + (number of elements * size[=length+content-char])
            int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code +  variable width for array size + other encoded payload
            byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0xB3); // 'sym32' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                Symbol symbol = source[i];
                assertEquals("Unexpected length", 1, symbol.length());

                expectedEncodingWrapper.putInt(1); // Length
                expectedEncodingWrapper.put((byte) symbol.toString().charAt(0)); // Content
            }

            assertEquals("Should have filled expected encoding array", 0, expectedEncodingWrapper.remaining());

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

    @Test
    public void testEncodeDecodeULongArray0() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULongs(0));
    }

    @Test
    public void testEncodeDecodeULongArray31() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULongs(31));
    }

    @Test
    public void testEncodeDecodeULongArray32() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULongs(32));
    }

    @Test
    public void testEncodeDecodeULongArray100() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULongs(100));
    }

    @Test
    public void testEncodeDecodeULong0Array0() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULong(UnsignedLong.ZERO, 0));
    }

    @Test
    public void testEncodeDecodeULong0Array100() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULong(UnsignedLong.ZERO, 100));
    }

    @Test
    public void testEncodeDecodeULongArray384() throws Throwable {
        doEncodeDecodeULongArrayTestImpl(createPayloadArrayULongs(384));
    }

    private void doEncodeDecodeULongArrayTestImpl(final UnsignedLong[] source) throws Throwable {
        final int count = source.length;

        try {
            final int encodingWidth = count < 32 ? 1 : 4;
            final int arrayPayloadSize =  encodingWidth + 1 + (8 * count); // count + element type descriptor + (element-width * number of elements)
            final int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + size + encoded payload
            final byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x80); // 'ulong' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putLong(source[i].longValue());
            }

            assertEquals("Should have filled expected encoding array", 0, expectedEncodingWrapper.remaining());

            // Now produce actual encoding of the array for later comparison
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);

            byte[] actualEncoding = new byte[buffer.position()];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertFalse(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(UnsignedLong.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (UnsignedLong[]) decoded);

            // Now verify expected encoding against the actual encoding of the array
            assertEquals("Unexpected encoded payload length", expectedEncoding.length, actualEncoding.length);
            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static UnsignedLong[] createPayloadArrayULongs(int length) {
        return createPayloadArrayULongs(length, false);
    }

    private static UnsignedLong[] createPayloadArrayULongs(int length, boolean smallUlongs) {
        Random rand = new Random(System.currentTimeMillis());

        UnsignedLong[] payload = new UnsignedLong[length];
        for (int i = 0; i < length; i++) {
            final long val;
            if(smallUlongs) {
                val = (long) rand.nextInt(256);
            } else {
                val = (long) rand.nextInt();
            }

            payload[i] = UnsignedLong.valueOf(val);
        }

        return payload;
    }

    private static UnsignedLong[] createPayloadArrayULong(final UnsignedLong value, final int length) {
        UnsignedLong[] payload = new UnsignedLong[length];
        for (int i = 0; i < length; i++) {
            payload[i] = value;
        }

        return payload;
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only0() throws Throwable {
        Object decoded = doDecodeULongArrayUlong0OnlyTestImpl(true, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(UnsignedLong.class, decoded.getClass().getComponentType());
        assertArrayEquals(new UnsignedLong[0], (UnsignedLong[]) decoded);
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only0Array32() throws Throwable {
        Object decoded = doDecodeULongArrayUlong0OnlyTestImpl(false, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(UnsignedLong.class, decoded.getClass().getComponentType());
        assertArrayEquals(new UnsignedLong[0], (UnsignedLong[]) decoded);
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only255() throws Throwable {
        try {
            doDecodeULongArrayUlong0OnlyTestImpl(true, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only255WhenAllowed() throws Throwable {
        doDecodeULongArrayUlong0OnlyWhenAllowedTestImpl(true, 255);
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only255Array32() throws Throwable {
        try {
            doDecodeULongArrayUlong0OnlyTestImpl(false, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only255Array32WhenAllowed() throws Throwable {
        doDecodeULongArrayUlong0OnlyWhenAllowedTestImpl(false, 255);
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only256() throws Throwable {
        try {
            doDecodeULongArrayUlong0OnlyTestImpl(false, 256);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only256WhenAllowed() throws Throwable {
        doDecodeULongArrayUlong0OnlyWhenAllowedTestImpl(false, 256);
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only384() throws Throwable {
        try {
            doDecodeULongArrayUlong0OnlyTestImpl(false, 384);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeULongArrayExplicitlyUlong0Only384WhenAllowed() throws Throwable {
        doDecodeULongArrayUlong0OnlyWhenAllowedTestImpl(false, 256);
    }

    private Object doDecodeULongArrayUlong0OnlyTestImpl(final boolean array8, final int count) throws Exception {
        if (array8) {
            assertTrue("Cant encode ulong0 array8 with count > 255", count <= 255);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1; // variable width for element count + byte type descriptor
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload
        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x44); // 'ulong0' type descriptor code

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        return decoder.readObject();
    }

    private void doDecodeULongArrayUlong0OnlyWhenAllowedTestImpl(final boolean array8, final int count) throws Exception {
        decoder.setZeroWidthArrayElementLimit(count);
        Object decoded = doDecodeULongArrayUlong0OnlyTestImpl(array8, count);

        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(UnsignedLong.class, decoded.getClass().getComponentType());

        UnsignedLong[] expected = new UnsignedLong[count];
        for (int i = 0; i < count; i++) {
            expected[i] = UnsignedLong.ZERO;
        }
        assertArrayEquals(expected, (UnsignedLong[]) decoded);
    }

    @Test
    public void testDecodeSmallULongArray253() throws Throwable {
        final int count = 253;
        doDecodeSmallULongArrayTestImpl(true, count, createPayloadArrayULongs(count, true));
        buffer.clear();
        doDecodeSmallULongArrayTestImpl(false, count, createPayloadArrayULongs(count, true));
    }

    @Test
    public void testDecodeSmallULongArray254() throws Throwable {
        final int count = 254;
        doDecodeSmallULongArrayTestImpl(false, count, createPayloadArrayULongs(count, true));
    }

    @Test
    public void testDecodeSmallULongArray255() throws Throwable {
        final int count = 255;
        doDecodeSmallULongArrayTestImpl(false, count, createPayloadArrayULongs(count, true));
    }

    @Test
    public void testDecodeSmallULongArray384() throws Throwable {
        final int count = 384;
        doDecodeSmallULongArrayTestImpl(false, count, createPayloadArrayULongs(count, true));
    }

    private void doDecodeSmallULongArrayTestImpl(final boolean array8, final int count, UnsignedLong[] smallULongs) throws Exception {
        if (array8) {
            assertTrue("Cant encode small-ulong array8 with count " + count, count <= 253);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1 + (count * 1); // variable width for element count + byte type descriptor + elements
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload

        if (array8) {
            assertTrue("Cant encode array8 with arrayPayloadSize " + count, arrayPayloadSize < 256);
        }

        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x53); // 'small-ulong' type descriptor code
        for(UnsignedLong element : smallULongs) {
            assertTrue(element.longValue() <= 255 && element.longValue() >= 0);

            buffer.put((byte) element.longValue());
        }

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        Object o = decoder.readObject();

        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertEquals(UnsignedLong.class, o.getClass().getComponentType());
        final UnsignedLong[] decoded = (UnsignedLong[]) o;

        assertArrayEquals(smallULongs, decoded);
    }

    @Test
    public void testEncodeDecodeUIntArray0() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInts(0));
    }

    @Test
    public void testEncodeDecodeUIntArray63() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInts(63));
    }

    @Test
    public void testEncodeDecodeUIntArray64() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInts(64));
    }

    @Test
    public void testEncodeDecodeUIntArray100() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInts(100));
    }

    @Test
    public void testEncodeDecodeUInt0Array0() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInt(UnsignedInteger.ZERO, 0));
    }

    @Test
    public void testEncodeDecodeUInt0Array100() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInt(UnsignedInteger.ZERO, 100));
    }

    @Test
    public void testEncodeDecodeUIntArray384() throws Throwable {
        doEncodeDecodeUIntArrayTestImpl(createPayloadArrayUInts(384));
    }

    @Test
    public void testDecodeSmallUIntArray253() throws Throwable {
        final int count = 253;
        doDecodeSmallUIntArrayTestImpl(true, count, createPayloadArrayUInts(count, true));
        buffer.clear();
        doDecodeSmallUIntArrayTestImpl(false, count, createPayloadArrayUInts(count, true));
    }

    @Test
    public void testDecodeSmallUIntArray254() throws Throwable {
        final int count = 254;
        doDecodeSmallUIntArrayTestImpl(false, count, createPayloadArrayUInts(count, true));
    }

    @Test
    public void testDecodeSmallUIntArray255() throws Throwable {
        final int count = 255;
        doDecodeSmallUIntArrayTestImpl(false, count, createPayloadArrayUInts(count, true));
    }

    @Test
    public void testDecodeSmallUIntArray384() throws Throwable {
        final int count = 384;
        doDecodeSmallUIntArrayTestImpl(false, count, createPayloadArrayUInts(count, true));
    }

    private void doDecodeSmallUIntArrayTestImpl(final boolean array8, final int count, UnsignedInteger[] smallUInts) throws Exception {
        if (array8) {
            assertTrue("Cant encode small-uint array8 with count " + count, count <= 253);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1 + (count * 1); // variable width for element count + byte type descriptor + elements
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload

        if (array8) {
            assertTrue("Cant encode array8 with arrayPayloadSize " + count, arrayPayloadSize < 256);
        }

        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x52); // 'small-uint' type descriptor code
        for(UnsignedInteger element : smallUInts) {
            assertTrue(element.intValue() <= 255 && element.intValue() >= 0);

            buffer.put((byte) element.intValue());
        }

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        Object o = decoder.readObject();

        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertEquals(UnsignedInteger.class, o.getClass().getComponentType());
        final UnsignedInteger[] decoded = (UnsignedInteger[]) o;

        assertArrayEquals(smallUInts, decoded);
    }

    private void doEncodeDecodeUIntArrayTestImpl(final UnsignedInteger[] source) throws Throwable {
        final int count = source.length;

        try {
            final int encodingWidth = count < 64 ? 1 : 4; // less than 64, since each element is 4 bytes, but we also need 1 byte for element count, and (in this case) 1 byte for primitive element type constructor.
            final int arrayPayloadSize =  encodingWidth + 1 + (4 * count); // count + element type descriptor + (element-width * number of elements)
            final int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + size + encoded payload
            final byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0x70); // 'uint' type descriptor code

            // Write the elements
            for (int i = 0; i < count; i++) {
                expectedEncodingWrapper.putInt(source[i].intValue());
            }

            assertEquals("Should have filled expected encoding array", 0, expectedEncodingWrapper.remaining());

            // Now produce actual encoding of the array for later comparison
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);

            byte[] actualEncoding = new byte[buffer.position()];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertFalse(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(UnsignedInteger.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (UnsignedInteger[]) decoded);

            // Now verify expected encoding against the actual encoding of the array
            assertEquals("Unexpected encoded payload length", expectedEncoding.length, actualEncoding.length);
            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static UnsignedInteger[] createPayloadArrayUInts(int length) {
        return createPayloadArrayUInts(length, false);
    }

    private static UnsignedInteger[] createPayloadArrayUInts(int length, boolean smallUints) {
        Random rand = new Random(System.currentTimeMillis());

        UnsignedInteger[] payload = new UnsignedInteger[length];
        for (int i = 0; i < length; i++) {
            final int val;
            if(smallUints) {
                val = rand.nextInt(256);
            } else {
                val = rand.nextInt();
            }

            payload[i] = UnsignedInteger.valueOf(val);
        }

        return payload;
    }

    private static UnsignedInteger[] createPayloadArrayUInt(final UnsignedInteger value, final int length) {
        UnsignedInteger[] payload = new UnsignedInteger[length];
        for (int i = 0; i < length; i++) {
            payload[i] = value;
        }

        return payload;
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only0() throws Throwable {
        Object decoded = doDecodeUIntArrayUInt0OnlyTestImpl(true, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(UnsignedInteger.class, decoded.getClass().getComponentType());
        assertArrayEquals(new UnsignedInteger[0], (UnsignedInteger[]) decoded);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only0Array32() throws Throwable {
        Object decoded = doDecodeUIntArrayUInt0OnlyTestImpl(false, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(UnsignedInteger.class, decoded.getClass().getComponentType());
        assertArrayEquals(new UnsignedInteger[0], (UnsignedInteger[]) decoded);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only255() throws Throwable {
        try {
            doDecodeUIntArrayUInt0OnlyTestImpl(true, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only255WhenAllowed() throws Throwable {
        doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(true, 255);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only255Array32() throws Throwable {
        try {
            doDecodeUIntArrayUInt0OnlyTestImpl(false, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only255Array32WhenAllowed() throws Throwable {
        doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(false, 255);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only256() throws Throwable {
        try {
            doDecodeUIntArrayUInt0OnlyTestImpl(false, 256);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only256WhenAllowed() throws Throwable {
        doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(false, 256);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only384() throws Throwable {
        try {
            doDecodeUIntArrayUInt0OnlyTestImpl(false, 384);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only384WhenAllowed() throws Throwable {
        doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(false, 384);
    }

    private Object doDecodeUIntArrayUInt0OnlyTestImpl(final boolean array8, final int count) throws Exception {
        if (array8) {
            assertTrue("Cant encode uint0 array8 with count > 255", count <= 255);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1; // variable width for element count + byte type descriptor
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload
        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x43); // 'uint0' type descriptor code

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        return decoder.readObject();
    }


    private void doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(final boolean array8, final int count) throws Exception {
        decoder.setZeroWidthArrayElementLimit(count);
        Object decoded = doDecodeUIntArrayUInt0OnlyTestImpl(array8, count);

        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(UnsignedInteger.class, decoded.getClass().getComponentType());

        UnsignedInteger[] expected = new UnsignedInteger[count];
        for (int i = 0; i < count; i++) {
            expected[i] = UnsignedInteger.ZERO;
        }
        assertArrayEquals(expected, (UnsignedInteger[]) decoded);
    }

    @Test
    public void testEncodeDecodeListArray19() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayLists(19, 1), 1);
    }

    @Test
    public void testEncodeDecodeListArray20() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayLists(20, 1), 1);
    }

    @Test
    public void testEncodeDecodeListArray100() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayLists(100, 1), 1);
    }

    @Test
    public void testEncodeDecodeListArray384() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayLists(384, 1), 1);
    }

    @Test
    public void testEncodeDecodeListEmptyArray0() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayEmptyList(0), 0);
    }

    @Test
    public void testEncodeDecodeListEmptyArray31() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayEmptyList(31), 0);
    }

    @Test
    public void testEncodeDecodeListEmptyArray32() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayEmptyList(32), 0);
    }

    @Test
    public void testEncodeDecodeListEmptyArray100() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayEmptyList(100), 0);
    }

    @Test
    public void testEncodeDecodeListEmptyArray384() throws Throwable {
        doEncodeDecodeListArrayTestImpl(createPayloadArrayEmptyList(384), 0);
    }

    private void doEncodeDecodeListArrayTestImpl(final List<Integer>[] source, final int expectedElementListLength) throws Throwable {
        final int count = source.length;
        final boolean emptyLists = expectedElementListLength == 0;

        try {
            final int encodingWidth;
            if(emptyLists) {
                encodingWidth = count < 32 ? 1 : 4; // less than 32, since each element is 8 bytes (list 4 size, 4 count), but we also need 1 for element count, and (in this case) 1 byte for primitive element type constructor.
            } else {
                encodingWidth = count < 20 ? 1 : 4; // less than 20, since each element is 13 bytes (list 4 size, 4 count, sub-value int with 1 element constructor, 4 value),
                                                    // but we also need 1 for element count, and (in this case) 1 byte for primitive element type constructor.
            }

            final int arrayPayloadSize =  encodingWidth + 1 + ((4 + 4 + ((1+4) * expectedElementListLength)) * count); // count + element type descriptor + (element-width * number of elements)
            final int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + size + encoded payload
            final byte[] expectedEncoding = new byte[expectedEncodedArraySize];
            ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

            // Write the array encoding code, array size, and element count
            if(encodingWidth == 1) {
                expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
                expectedEncodingWrapper.put((byte) arrayPayloadSize);
                expectedEncodingWrapper.put((byte) count);
            } else {
                expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
                expectedEncodingWrapper.putInt(arrayPayloadSize);
                expectedEncodingWrapper.putInt(count);
            }

            // Write the type descriptor
            expectedEncodingWrapper.put((byte) 0xD0); // 'list32' type descriptor code

            // Write the elements (lists)
            for (int i = 0; i < count; i++) {
                final List<Integer> list = source[i];
                assertEquals(expectedElementListLength, list.size());

                expectedEncodingWrapper.putInt(4 + ((1+4) * expectedElementListLength)); // size = count + elements
                expectedEncodingWrapper.putInt(expectedElementListLength);
                for(int j = 0; j < expectedElementListLength; j++) {
                    expectedEncodingWrapper.put((byte) 0x71); // 'int' type descriptor code
                    expectedEncodingWrapper.putInt(list.get(0));
                }
            }

            assertEquals("Should have filled expected encoding array", 0, expectedEncodingWrapper.remaining());

            // Now produce actual encoding of the array for later comparison
            assertEquals("Unexpected buffer position", 0, buffer.position());
            encoder.writeArray(source);

            byte[] actualEncoding = new byte[buffer.position()];
            buffer.flip();
            buffer.get(actualEncoding);
            assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

            // Now verify against the decoding
            buffer.flip();
            Object decoded = decoder.readObject();
            assertNotNull(decoded);
            assertTrue(decoded.getClass().isArray());
            assertFalse(decoded.getClass().getComponentType().isPrimitive());
            assertEquals(List.class, decoded.getClass().getComponentType());

            assertArrayEquals("Unexpected decoding", source, (List<Integer>[]) decoded);

            // Now verify expected encoding against the actual encoding of the array
            assertEquals("Unexpected encoded payload length", expectedEncoding.length, actualEncoding.length);
            assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);
        }
        catch (Throwable t) {
            System.err.println("Error during test, source array: " + Arrays.toString(source));
            throw t;
        }
    }

    private static List<Integer>[] createPayloadArrayLists(int arrayLength, int listLength) {
        Random rand = new Random(System.currentTimeMillis());

        List<?>[] payload = new ArrayList<?>[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            ArrayList<Integer> l = new ArrayList<Integer>();
            for (int j = 0; j < listLength; j++) {
                l.add(256 + rand.nextInt(1024)); // adding 256 ensures it is a full size int every time, consistent sizing
            }

            payload[i] = l;
        }

        return (List<Integer>[]) payload;
    }

    private static List<Integer>[] createPayloadArrayEmptyList(final int length) {
        List<?>[] payload = new List<?>[length];
        for (int i = 0; i < length; i++) {
            payload[i] = new ArrayList<Integer>(0);
        }

        return (List<Integer>[]) payload;
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only0() throws Throwable {
        Object decoded = doDecodeListArrayList0OnlyTestImpl(true, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(List.class, decoded.getClass().getComponentType());
        assertArrayEquals(new List[0], (List[]) decoded);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only0Array32() throws Throwable {
        Object decoded = doDecodeListArrayList0OnlyTestImpl(false, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(List.class, decoded.getClass().getComponentType());
        assertArrayEquals(new ArrayList[0], (List[]) decoded);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only254() throws Throwable {
        try {
            doDecodeListArrayList0OnlyTestImpl(true, 254);
            fail("Expected exception");
        } catch (DecodeException e) {
            assertEquals("Array element count 254 is specified to be greater than the allowed 0-width-element limit for arrays (0)", e.getMessage());
        }
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only254WhenAllowed() throws Throwable {
        doDecodeListArrayList0OnlyWhenAllowedTestImpl(true, 254);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only254Array32() throws Throwable {
        try {
            doDecodeListArrayList0OnlyTestImpl(false, 254);
            fail("Expected exception");
        } catch (DecodeException e) {
            assertEquals("Array element count 254 is specified to be greater than the allowed 0-width-element limit for arrays (0)", e.getMessage());
        }
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only254Array32WhenAllowed() throws Throwable {
        doDecodeListArrayList0OnlyWhenAllowedTestImpl(false, 254);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only255() throws Throwable {
        try {
            doDecodeListArrayList0OnlyTestImpl(true, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only255WhenAllowed() throws Throwable {
        doDecodeListArrayList0OnlyWhenAllowedTestImpl(true, 255);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only255Array32() throws Throwable {
        try {
            doDecodeListArrayList0OnlyTestImpl(false, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only255Array32WhenAllowed() throws Throwable {
        doDecodeListArrayList0OnlyWhenAllowedTestImpl(false, 255);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only384() throws Throwable {
        try {
            doDecodeListArrayList0OnlyTestImpl(false, 384);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only384WhenAllowed() throws Throwable {
        doDecodeListArrayList0OnlyWhenAllowedTestImpl(false, 384);
    }

    private void doDecodeListArrayList0OnlyWhenAllowedTestImpl(final boolean array8, final int count) throws Exception {
        decoder.setZeroWidthArrayElementLimit(count);
        Object decoded = doDecodeListArrayList0OnlyTestImpl(array8, count);

        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(List.class, decoded.getClass().getComponentType());

        List<?>[] expected = new List<?>[count];
        for (int i = 0; i < count; i++) {
            expected[i] = Collections.emptyList();
        }

        assertArrayEquals(expected, (List<?>[]) decoded);
    }

    private Object doDecodeListArrayList0OnlyTestImpl(final boolean array8, final int count) throws Exception {
        if (array8) {
            assertTrue("Cant encode array8 of list0 with count > 255", count < 256);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1; // variable width for element count + byte type descriptor
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload
        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x45); // 'list0' type descriptor code

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        return decoder.readObject();
    }

    @Test
    public void testDecodeNullArray0() throws Throwable {
        Object decoded = doDecodeNullArrayTestImpl(true, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(Void.class, decoded.getClass().getComponentType());
        assertArrayEquals(new Void[0], (Void[]) decoded);
    }

    @Test
    public void testDecodeNullArray0Array32() throws Throwable {
        Object decoded = doDecodeNullArrayTestImpl(false, 0);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(Void.class, decoded.getClass().getComponentType());
        assertArrayEquals(new Void[0], (Void[]) decoded);
    }

    @Test
    public void testDecodeNullArray255() throws Throwable {
        try {
            doDecodeNullArrayTestImpl(true, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeNullArray255WhenAllowed() throws Throwable {
        doDecodeNullArrayWhenAllowedTestImpl(true, 255);
    }

    @Test
    public void testDecodeNullArray255Array32() throws Throwable {
        try {
            doDecodeNullArrayTestImpl(false, 255);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeNullArray255Array32WhenAllowed() throws Throwable {
        doDecodeNullArrayWhenAllowedTestImpl(false, 255);
    }

    @Test
    public void testDecodeNullArray256() throws Throwable {
        try {
            doDecodeNullArrayTestImpl(false, 256);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeNullArray256WhenAllowed() throws Throwable {
        doDecodeNullArrayWhenAllowedTestImpl(false, 256);
    }

    @Test
    public void testDecodeNullArray384() throws Throwable {
        try {
            doDecodeNullArrayTestImpl(false, 384);
            fail("Expected exception");
        } catch (DecodeException e) {
            // expected
        }
    }

    @Test
    public void testDecodeNullArray384WhenAllowed() throws Throwable {
        doDecodeNullArrayWhenAllowedTestImpl(false, 384);
    }

    private void doDecodeNullArrayWhenAllowedTestImpl(boolean array8, final int count) throws Throwable {
        decoder.setZeroWidthArrayElementLimit(count);
        Object decoded = doDecodeNullArrayTestImpl(array8, count);
        assertNotNull(decoded);
        assertTrue(decoded.getClass().isArray());
        assertEquals(Void.class, decoded.getClass().getComponentType());

        final Void[] expected = new Void[count];

        assertArrayEquals(expected, (Void[]) decoded);
    }

    private Object doDecodeNullArrayTestImpl(final boolean array8, final int count) throws Exception {
        if (array8) {
            assertTrue("Cant encode null array8 with count > 255", count < 256);
        }

        int encodingWidth = array8 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1; // variable width for element count + byte type descriptor
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload
        // Write the array encoding code, array size, and element count
        if (array8) {
            buffer.put((byte) 0xE0); // 'array8' type descriptor code
            buffer.put((byte) arrayPayloadSize);
            buffer.put((byte) count);
        } else {
            buffer.put((byte) 0xF0); // 'array32' type descriptor code
            buffer.putInt(arrayPayloadSize);
            buffer.putInt(count);
        }

        buffer.put((byte) 0x40); // 'null' type descriptor code

        assertEquals(expectedEncodedArraySize, buffer.position());

        buffer.flip();

        return decoder.readObject();
    }

    @Test
    public void testEncodeNullArray0() throws Throwable {
        doEncodeNullArrayTestImpl(0);
    }

    @Test
    public void testEncodeNullArray1() throws Throwable {
        try {
            doEncodeNullArrayTestImpl(1);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testEncodeNullArray256() throws Throwable {
        try {
            doEncodeNullArrayTestImpl(256);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    private void doEncodeNullArrayTestImpl(final int count) throws Exception {
        Void[] source = new Void[count];

        int encodingWidth = count < 256 ? 1 : 4;
        int arrayPayloadSize = encodingWidth + 1; // variable width for element count + byte type descriptor
        int expectedEncodedArraySize = 1 + encodingWidth + arrayPayloadSize; // array type code + variable width for
                                                                             // array size + other encoded payload
        final byte[] expectedEncoding = new byte[expectedEncodedArraySize];
        ByteBuffer expectedEncodingWrapper = ByteBuffer.wrap(expectedEncoding);

        // Write the array encoding code, array size, and element count
        if(encodingWidth == 1) {
            expectedEncodingWrapper.put((byte) 0xE0); // 'array8' type descriptor code
            expectedEncodingWrapper.put((byte) arrayPayloadSize);
            expectedEncodingWrapper.put((byte) count);
        } else {
            expectedEncodingWrapper.put((byte) 0xF0); // 'array32' type descriptor code
            expectedEncodingWrapper.putInt(arrayPayloadSize);
            expectedEncodingWrapper.putInt(count);
        }

        expectedEncodingWrapper.put((byte) 0x40); // 'null' type descriptor code

        assertEquals(expectedEncodedArraySize, expectedEncodingWrapper.position());
        expectedEncodingWrapper.flip();


        // Now produce actual encoding of the array for later comparison
        assertEquals("Unexpected buffer position", 0, buffer.position());
        encoder.writeArray(source);

        byte[] actualEncoding = new byte[buffer.position()];
        buffer.flip();
        buffer.get(actualEncoding);
        assertFalse("Should have drained the encoder buffer contents", buffer.hasRemaining());

        // Now verify expected encoding against the actual encoding of the array
        assertEquals("Unexpected encoded payload length", expectedEncoding.length, actualEncoding.length);
        assertArrayEquals("Unexpected actual array encoding", expectedEncoding, actualEncoding);
    }

    @Test
    public void testArrayOfArraysOfMixedTypes() throws IOException {
        final int size = 10;

        Object[][] source = new Object[2][size];
        for (int i = 0; i < size; ++i) {
            source[0][i] = Short.valueOf((short) i);
            source[1][i] = Integer.valueOf(i);
        }

        encoder.writeArray(source);

        buffer.flip();

        Object result = decoder.readObject(buffer);

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        Object[] resultArray = (Object[]) result;

        assertNotNull(resultArray);
        assertEquals(2, resultArray.length);

        assertTrue(resultArray[0].getClass().isArray());
        assertEquals(short.class, resultArray[0].getClass().getComponentType());
        assertTrue(resultArray[1].getClass().isArray());
        assertEquals(int.class, resultArray[1].getClass().getComponentType());
    }

    @Test
    public void testDecodeArrayOfDescribedExplicitTrue() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.BOOLEAN_TRUE;
        final Boolean described = Boolean.TRUE;
        final byte arrayType = EncodingCodes.ARRAY32;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedExplicitTrueArray8() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.BOOLEAN_TRUE;
        final Boolean described = Boolean.TRUE;
        final byte arrayType = EncodingCodes.ARRAY8;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedExplicitFalse() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.BOOLEAN_FALSE;
        final Boolean described = Boolean.FALSE;
        final byte arrayType = EncodingCodes.ARRAY32;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedULong0() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.ULONG0;
        final UnsignedLong described = UnsignedLong.ZERO;
        final byte arrayType = EncodingCodes.ARRAY32;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedUInt0() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.UINT0;
        final UnsignedInteger described = UnsignedInteger.ZERO;
        final byte arrayType = EncodingCodes.ARRAY32;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedNull() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.NULL;
        final Object described = null;
        final byte arrayType = EncodingCodes.ARRAY32;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedList0() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.LIST0;
        final List<Integer> described = new ArrayList<Integer>();
        final byte arrayType = EncodingCodes.ARRAY32;

        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedNested() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.DESCRIBED_TYPE_INDICATOR;

        try {
            doDecodeArrayOfDescribedZeroWidthTestImpl(EncodingCodes.ARRAY32, describedPrimitiveConstructorCode);
            fail("Expected exception");
        } catch(DecodeException de) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", de.getMessage());
        }

        buffer.clear();

        try {
            doDecodeArrayOfDescribedZeroWidthTestImpl(EncodingCodes.ARRAY8, describedPrimitiveConstructorCode);
            fail("Expected exception");
        } catch(DecodeException de) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", de.getMessage());
        }
    }

    private Object[] doDecodeArrayOfDescribedZeroWidthTestImpl(final byte arrayTypeCode, final byte describedPrimitiveConstructorCode) {
        if (!(describedPrimitiveConstructorCode == EncodingCodes.BOOLEAN_TRUE
                || describedPrimitiveConstructorCode == EncodingCodes.BOOLEAN_FALSE
                || describedPrimitiveConstructorCode == EncodingCodes.UINT0
                || describedPrimitiveConstructorCode == EncodingCodes.ULONG0
                || describedPrimitiveConstructorCode == EncodingCodes.LIST0
                || describedPrimitiveConstructorCode == EncodingCodes.NULL
                || describedPrimitiveConstructorCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR)) {
            throw new IllegalArgumentException("Expected a 0-width element encoding or described type indicator, got: "
                    + EncodingCodes.toString(describedPrimitiveConstructorCode));
        }

        if (arrayTypeCode == EncodingCodes.ARRAY32) {
            buffer.put(EncodingCodes.ARRAY32);
            buffer.putInt(4 + 1 + 1 + 1 + 10 + 1);  // Size
            buffer.putInt(2);  // Count
        } else {
            buffer.put(EncodingCodes.ARRAY8);
            buffer.put((byte) (1 + 1 + 1 + 1 + 10 + 1));  // Size
            buffer.put((byte) 2);  // Count
        }
        buffer.put((byte) 0); // Described Type Indicator
        buffer.put(EncodingCodes.SYM8); // sym8 type code for symbolic descriptor
        buffer.put((byte) 10); //symbol length
        buffer.put("DESCRIPTOR".getBytes(StandardCharsets.US_ASCII));
        buffer.put(describedPrimitiveConstructorCode); // ulong constructor type-code for described primitive
        // We dont populate any value bytes as we are expecting 0 length element constructors (or described type indicator to throw)

        byte[] expectedEncoding = new byte[buffer.position()];

        buffer.flip();

        buffer.get(expectedEncoding);
        assertEquals("Should have drained the encoder buffer contents", 0, buffer.remaining());

        buffer.flip();

        // First verify that it throws with the element limit default
        try {
            decoder.readObject();
            fail("Expected exception");
        } catch (DecodeException de) {
            // Expected
        }

        buffer.rewind();
        assertEquals("Should have expected buffer content remaining", expectedEncoding.length, buffer.remaining());

        // Now try again with permissive element limit
        decoder.setZeroWidthArrayElementLimit(2);

        Object o = decoder.readObject();
        assertNotNull(o);
        assertTrue(o.getClass().isArray());

        Object[] decoded = (Object[]) o;
        assertEquals(2, decoded.length);
        return decoded;
    }

    private void assertArrayOfDescribedTypeValues(Object[] decoded, final Object described) {
        assertTrue(decoded.length > 0);

        for (int i = 0; i < decoded.length; i++) {
            assertTrue(decoded[i] instanceof DescribedType);
            DescribedType dt = (DescribedType) decoded[i];
            assertEquals(Symbol.valueOf("DESCRIPTOR"), dt.getDescriptor());

            assertEquals(described, dt.getDescribed());
        }
    }

    @Test
    public void testDecodeIntegerArray() throws Exception {
        Object o = doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 26, 6);
        assertDecodedIntegerArray(o);

        o = doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 29, 6);
        assertDecodedIntegerArray(o);
    }

    private void assertDecodedIntegerArray(Object o) {
        assertNotNull(o);
        assertTrue(o.getClass().isArray());
        assertEquals(int.class, o.getClass().getComponentType());

        int[] array = (int[]) o;

        assertEquals(6, array.length);

        assertEquals(6, array[0]);
        assertEquals(5, array[1]);
        assertEquals(4, array[2]);
        assertEquals(3, array[3]);
        assertEquals(2, array[4]);
        assertEquals(1, array[5]);
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 26 + 1, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array size 27 cant be more than remaining bytes 26", de.getMessage());
        }

        try {
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 29 + 1, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array size 30 cant be more than remaining bytes 29", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array size -1 cant be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 29, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array element count -1 cant be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full Array, so the 'remaining' checks dont foul
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 2, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array element count 6 is specified to be greater than the size of array (2)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full Array, so the 'remaining' checks dont foul
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 5, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array element count 6 is specified to be greater than the size of array (5)", de.getMessage());
        }
    }

    private Object doDecodeIntegerArrayWithSpecificSizeCountTestImpl(final boolean array32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        if (array32) {
            tmpBuf.put((byte) 0xF0); // Array32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) 0xE0); // Array8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
        tmpBuf.put((byte) 0x71); // int constructor
        tmpBuf.putInt(6);
        tmpBuf.putInt(5);
        tmpBuf.putInt(4);
        tmpBuf.putInt(3);
        tmpBuf.putInt(2);
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

    @Test
    public void testDecodeArrayOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part Array, but count for full Array...will try to decode count, overflow
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 22 - 4, 5);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part Array, but count for full Array...will try to decode count, overflow
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 25 - 4, 5);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeArrayUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full Array, but count for part Array...will try to decode count, underflow
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 22, 5 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array incorrectly encoded, left 4 bytes from size indicated 22", de.getMessage());
        }

        try {
            // Deliberately pass size for full Array, but count for part Array...will try to decode count, underflow
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 25, 5 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Array incorrectly encoded, left 4 bytes from size indicated 25", de.getMessage());
        }
    }

    @Test
    public void testDecodeArrayFailsIfDepthLimitExceeded() throws Exception {
        final int depth = 2;
        decoder.setMaxDecodeDepth(1);

        final int width = 2;
        final int size = 10;
        Object[][] source = new Object[width][size];
        for (int i = 0; i < size; ++i) {
            source[0][i] = Short.valueOf((short) i);
            source[1][i] = Integer.valueOf(i);
        }

        encoder.writeObject(source);
        buffer.flip();
        buffer.mark();

        try {
            decoder.readObject();
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", de.getMessage());
        }

        decoder.setMaxDecodeDepth(depth);
        buffer.reset();

        try {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result.getClass().isArray());
            assertEquals(Object.class, result.getClass().getComponentType());

            Object[] decoded = (Object[]) result;
            assertEquals(width, decoded.length);

            assertTrue(decoded[0].getClass().isArray());
            assertEquals(short.class, decoded[0].getClass().getComponentType());

            assertTrue(decoded[1].getClass().isArray());
            assertEquals(int.class, decoded[1].getClass().getComponentType());
        } catch (Exception e) {
            fail("Should be able to read the array after depth configuration change");
        }
    }

    @Test
    public void testDecodeArrayOfListsOfListsFailsIfDepthLimitExceeded() throws Exception {
        decoder.setMaxDecodeDepth(1);

        List<List<?>> elementList = new ArrayList<>();
        elementList.add(Collections.emptyList());
        elementList.add(Collections.emptyList());
        elementList.add(Collections.emptyList());

        Object[] source = new Object[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = elementList;
        }

        encoder.writeArray(source);
        buffer.flip();
        buffer.mark();

        try {
            decoder.readObject();
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", de.getMessage());
        }

        buffer.reset();
        decoder.setMaxDecodeDepth(2);

        try {
            decoder.readObject();
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 2", de.getMessage());
        }

        buffer.reset();
        decoder.setMaxDecodeDepth(3);

        try {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result.getClass().isArray());
        } catch (Exception e) {
            fail("Should be able to read the array after depth configuration change");
        }
    }

    @Test
    public void testDecodeArrayOfListsOfMapsFailsIfDepthLimitExceeded() throws Exception {
        decoder.setMaxDecodeDepth(1);

        List<Map<?, ?>> elementList = new ArrayList<>();
        elementList.add(Collections.emptyMap());
        elementList.add(Collections.emptyMap());
        elementList.add(Collections.emptyMap());

        Object[] source = new Object[2];
        for (int i = 0; i < source.length; ++i) {
            source[i] = elementList;
        }

        encoder.writeArray(source);
        buffer.flip();
        buffer.mark();

        try {
            decoder.readObject();
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", de.getMessage());
        }

        buffer.reset();
        decoder.setMaxDecodeDepth(2);

        try {
            decoder.readObject();
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 2", de.getMessage());
        }

        buffer.reset();
        decoder.setMaxDecodeDepth(3);

        try {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result.getClass().isArray());
        } catch (Exception e) {
            fail("Should be able to read the array after depth configuration change");
        }
    }

    @Test
    public void testDecodingOfDeeplyNestedArrayOfArray32() throws Exception {
        doDecodingOfDeeplyNestedArrayOfArrayTestImpl(true, 10, true);
        doDecodingOfDeeplyNestedArrayOfArrayTestImpl(true, 10, false);
    }

    @Test
    public void testDecodingOfDeeplyNestedArrayOfArray8() throws Exception {
        doDecodingOfDeeplyNestedArrayOfArrayTestImpl(false, 10, true);
        doDecodingOfDeeplyNestedArrayOfArrayTestImpl(false, 10, false);
    }

    private void doDecodingOfDeeplyNestedArrayOfArrayTestImpl(final boolean array32, final int depth, boolean readObject) {
        ByteBuffer tmpBuf = buildNestedArraysBuffer(depth, array32);
        decoder.setByteBuffer(tmpBuf);

        decoder.setMaxDecodeDepth(depth - 1);

        try {
            if (readObject) {
                decoder.readObject();
            } else {
                decoder.readArray();
            }
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: " + (depth - 1), de.getMessage());
            //expected
        }

        decoder.setMaxDecodeDepth(depth);
        tmpBuf.clear();

        final Object result;
        if (readObject) {
            result = decoder.readObject();
        } else {
            result = decoder.readArray();
        }

        assertTrue(result instanceof Object[]);
    }

    private ByteBuffer buildNestedArraysBuffer(final int levels, boolean array32)
    {
        final int encodingWith = array32 ? 4 : 1;
        final int totalEncodingSize = ((1 + (encodingWith * 2)) * levels) + 1; // Number of bytes for size + count + 1 byte for array type + 1 for element constructor, times levels
                                                                               // + 1 for final element type constructor (final array is empty)

        ByteBuffer tmpBuf = ByteBuffer.allocate(totalEncodingSize);

        int currentArraySize = totalEncodingSize - encodingWith - 1; // Size does not include its own type code and size itself

        if(!array32) {
            assertTrue("Cant encode array8 of this depth within 255 byte array size limit", currentArraySize < 256);
        }

        final byte arrayTypeCode = array32 ? EncodingCodes.ARRAY32 : EncodingCodes.ARRAY8;
        final int individualArrayBytes = 1 + (encodingWith * 2); // type code, then size, then count

        for (int i = 1; i <= levels; i++)
        {
            // Type code
            tmpBuf.put(arrayTypeCode);

            // Size
            if(array32) {
                tmpBuf.putInt(currentArraySize);
            } else {
                tmpBuf.put((byte) currentArraySize);
            }

            if(i < levels) {
                // Count (1 for all but last, the nested array value coming next)
                if(array32) {
                    tmpBuf.putInt(1);
                } else {
                    tmpBuf.put((byte) 1);
                }

                // Value will be written on next iteration

                // Update encoded array size for next iteration
                currentArraySize -= individualArrayBytes;
            } else {
                // Count (0 for the final empty array)
                if(array32) {
                    tmpBuf.putInt(0);
                } else {
                    tmpBuf.put((byte) 0);
                }
            }
        }

        assertEquals(encodingWith + 1, currentArraySize); // count of prior array + the final list0 type constructor below

        tmpBuf.put(EncodingCodes.LIST0);

        // Check we filled the buffer
        assertEquals(0, tmpBuf.remaining());

        tmpBuf.flip();

        return tmpBuf;
    }
}