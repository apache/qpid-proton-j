/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.proton.codec.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnknownDescribedType;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.codec.CodecTestSupport;
import org.apache.qpid.proton.codec.Data;
import org.apache.qpid.proton.codec.Data.DataType;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.junit.Test;

public class DataImplTest extends CodecTestSupport
{
    @Test
    public void testEncodeDecodeSymbolArrayUsingPutArray()
    {
        Symbol symbol1 = Symbol.valueOf("testRoundtripSymbolArray1");
        Symbol symbol2 = Symbol.valueOf("testRoundtripSymbolArray2");

        Data data1 = new DataImpl();
        data1.putArray(false, Data.DataType.SYMBOL);
        data1.enter();
        data1.putSymbol(symbol1);
        data1.putSymbol(symbol2);
        data1.exit();

        Binary encoded = data1.encode();
        encoded.asByteBuffer();

        Data data2 = new DataImpl();
        data2.decode(encoded.asByteBuffer());

        assertEquals("unexpected array length", 2, data2.getArray());
        assertEquals("unexpected array length", Data.DataType.SYMBOL, data2.getArrayType());

        Object[] array = data2.getJavaArray();
        assertNotNull("Array should not be null", array);
        assertEquals("Expected a Symbol array", Symbol[].class, array.getClass());
        assertEquals("unexpected array length", 2, array.length);
        assertEquals("unexpected value", symbol1, array[0]);
        assertEquals("unexpected value", symbol2, array[1]);
    }

    @Test
    public void testEncodeDecodeSymbolArrayUsingPutObject()
    {
        Symbol symbol1 = Symbol.valueOf("testRoundtripSymbolArray1");
        Symbol symbol2 = Symbol.valueOf("testRoundtripSymbolArray2");
        Symbol[] input = new Symbol[]{symbol1, symbol2};

        Data data1 = new DataImpl();
        data1.putObject(input);

        Binary encoded = data1.encode();
        encoded.asByteBuffer();

        Data data2 = new DataImpl();
        data2.decode(encoded.asByteBuffer());

        assertEquals("unexpected array length", 2, data2.getArray());
        assertEquals("unexpected array length", Data.DataType.SYMBOL, data2.getArrayType());

        Object[] array = data2.getJavaArray();
        assertArrayEquals("Array not as expected", input, array);
    }

    @Test
    public void testEncodeArrayOfLists()
    {
        // encode an array of two empty lists
        Data data = new DataImpl();
        data.putArray(false, Data.DataType.LIST);
        data.enter();
        data.putList();
        data.putList();
        data.exit();

        int expectedEncodedSize = 4; // 1b type + 1b size + 1b length + 1b element constructor


        Binary encoded = data.encode();
        assertEquals("unexpected encoding size", expectedEncodedSize, encoded.getLength());

        ByteBuffer expectedEncoding = ByteBuffer.allocate(expectedEncodedSize);
        expectedEncoding.put((byte)0xe0);   // constructor
        expectedEncoding.put((byte)2);   // size
        expectedEncoding.put((byte)2);   // count
        expectedEncoding.put((byte)0x45);   // element constructor

        assertEquals("unexpected encoding", new Binary(expectedEncoding.array()), encoded);

        data = new DataImpl();
        data.putArray(false, Data.DataType.LIST);
        data.enter();
        data.putList();
        data.putList();
        data.putList();
        data.enter();
        data.putNull();
        data.exit();
        data.exit();

        expectedEncodedSize = 11; // 1b type + 1b size + 1b length + 1b element constructor + 3 * (1b size + 1b count) + 1b null elt

        encoded = data.encode();
        assertEquals("unexpected encoding size", expectedEncodedSize, encoded.getLength());

        expectedEncoding = ByteBuffer.allocate(expectedEncodedSize);
        expectedEncoding.put((byte)0xe0);   // constructor
        expectedEncoding.put((byte)9);   // size
        expectedEncoding.put((byte)3);   // count
        expectedEncoding.put((byte)0xc0);   // element constructor
        expectedEncoding.put((byte)1);   // size
        expectedEncoding.put((byte)0);   // count
        expectedEncoding.put((byte)1);   // size
        expectedEncoding.put((byte)0);   // count
        expectedEncoding.put((byte)2);   // size
        expectedEncoding.put((byte)1);   // count
        expectedEncoding.put((byte)0x40);   // null value

        assertEquals("unexpected encoding", new Binary(expectedEncoding.array()), encoded);

        data = new DataImpl();
        data.putArray(false, Data.DataType.LIST);
        data.enter();
        data.putList();
        data.putList();
        data.putList();
        data.enter();
        for(int i = 0; i < 256; i++)
        {
            data.putNull();
        }
        data.exit();
        data.exit();

        expectedEncodedSize = 290; // 1b type + 4b size + 4b length + 1b element constructor + 3 * (4b size + 4b count) + (256 * 1b) null elt
        encoded = data.encode();
        assertEquals("unexpected encoding size", expectedEncodedSize, encoded.getLength());

        expectedEncoding = ByteBuffer.allocate(expectedEncodedSize);
        expectedEncoding.put((byte)0xf0);   // constructor
        expectedEncoding.putInt(285);   // size
        expectedEncoding.putInt(3);   // count
        expectedEncoding.put((byte)0xd0);   // element constructor
        expectedEncoding.putInt(4);   // size
        expectedEncoding.putInt(0);   // count
        expectedEncoding.putInt(4);   // size
        expectedEncoding.putInt(0);   // count
        expectedEncoding.putInt(260);   // size
        expectedEncoding.putInt(256);   // count
        for(int i = 0; i < 256; i++)
        {
            expectedEncoding.put((byte)0x40);   // null value
        }

        assertEquals("unexpected encoding", new Binary(expectedEncoding.array()), encoded);

    }

    @Test
    public void testEncodeArrayOfMaps()
    {
        // encode an array of two empty maps
        Data data = new DataImpl();
        data.putArray(false, Data.DataType.MAP);
        data.enter();
        data.putMap();
        data.putMap();
        data.exit();

        int expectedEncodedSize = 8; // 1b type + 1b size + 1b length + 1b element constructor + 2 * (1b size + 1b count)


        Binary encoded = data.encode();
        assertEquals("unexpected encoding size", expectedEncodedSize, encoded.getLength());

        ByteBuffer expectedEncoding = ByteBuffer.allocate(expectedEncodedSize);
        expectedEncoding.put((byte) 0xe0);   // constructor
        expectedEncoding.put((byte) 6);   // size
        expectedEncoding.put((byte) 2);   // count
        expectedEncoding.put((byte) 0xc1);   // element constructor
        expectedEncoding.put((byte)1);   // size
        expectedEncoding.put((byte)0);   // count
        expectedEncoding.put((byte)1);   // size
        expectedEncoding.put((byte)0);   // count


        assertEquals("unexpected encoding", new Binary(expectedEncoding.array()), encoded);

    }

    @Test
    public void testEncodeString32()
    {
        byte[] strPayload = createStringPayloadBytes(256);
        String content = new String(strPayload, StandardCharsets.UTF_8);
        assertTrue("Length must be over 255 to ensure use of str32 encoding", content.length() > 255);

        int encodedSize = 1 + 4 + strPayload.length; // 1b type + 4b length + content
        ByteBuffer expectedEncoding = ByteBuffer.allocate(encodedSize);
        expectedEncoding.put((byte) 0xB1);
        expectedEncoding.putInt(strPayload.length);
        expectedEncoding.put(strPayload);

        Data data = new DataImpl();
        data.putString(content);

        Binary encoded = data.encode();

        assertEquals("unexpected encoding", new Binary(expectedEncoding.array()), encoded);
    }

    @Test
    public void testEncodeDecodeString32()
    {
        byte[] payload = createStringPayloadBytes(1025);
        String content = new String(payload, StandardCharsets.UTF_8);
        assertTrue("Length must be over 255 to ensure use of str32 encoding", content.length() > 255);

        doEncodeDecodeStringTestImpl(content);
    }

    @Test
    public void testEncodeDecodeString8()
    {
        String content = "testRoundTripString8";
        assertTrue("Length must be <= 255 to allow use of str8 encoding", content.length() <= 255);

        doEncodeDecodeStringTestImpl("testRoundTripString8");
    }

    private void doEncodeDecodeStringTestImpl(String string)
    {
        Data data = new DataImpl();
        data.putString(string);

        Binary encoded = data.encode();

        Data data2 = new DataImpl();
        data2.decode(encoded.asByteBuffer());

        assertEquals("unexpected type", Data.DataType.STRING, data2.type());
        assertEquals("unexpected string", string, data2.getString());
    }

    private byte[] createStringPayloadBytes(int length)
    {
        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) ((i % 10) + 48);
        }

        return payload;
    }

    @Test
    public void testEncodeStringBinary32()
    {
        byte[] payload = createStringPayloadBytes(1372);
        assertTrue("Length must be over 255 to ensure use of vbin32 encoding", payload.length > 255);

        int encodedSize = 1 + 4 + payload.length; // 1b type + 4b length + content
        ByteBuffer expectedEncoding = ByteBuffer.allocate(encodedSize);
        expectedEncoding.put((byte) 0xB0);
        expectedEncoding.putInt(payload.length);
        expectedEncoding.put(payload);

        Data data = new DataImpl();
        data.putBinary(new Binary(payload));

        Binary encoded = data.encode();

        assertEquals("unexpected encoding", new Binary(expectedEncoding.array()), encoded);
    }

    @Test
    public void testEncodeDecodeBinary32()
    {
        byte[] initialPayload = createStringPayloadBytes(1025);
        String initialContent = new String(initialPayload, StandardCharsets.UTF_8);
        assertTrue("Length must be over 255 to ensure use of str32 encoding", initialContent.length() > 255);

        byte[] bytesReadBack = doEncodeDecodeBinaryTestImpl(initialPayload);
        String readBackContent = new String(bytesReadBack, StandardCharsets.UTF_8);
        assertEquals(initialContent, readBackContent);
    }

    private byte[] doEncodeDecodeBinaryTestImpl(byte[] payload)
    {
        Data data = new DataImpl();
        data.putBinary(payload);

        Binary encoded = data.encode();

        ByteBuffer byteBuffer = encoded.asByteBuffer();
        Data data2 = new DataImpl();
        long decodeResult = data2.decode(byteBuffer);
        assertTrue(Long.toString(decodeResult), decodeResult > 0);

        assertEquals("unexpected type", Data.DataType.BINARY, data2.type());
        return data2.getBinary().getArray();
    }

    @Test
    public void testDecodeArrayOfBoolean() throws IOException {
        final int size = 10;

        boolean[] source = new boolean[size];
        for (int i = 0; i < size; ++i) {
            source[i] = i % 2 == 0;
        }

        encoder.writeArray(source);

        buffer.flip();

        DataImpl dataImpl = new DataImpl();
        dataImpl.decode(buffer);

        assertEquals("unexpected array length", size, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.BOOL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", size, decoded.length);

        for (int i = 0; i < size; ++i) {
            assertEquals("unexpected value", source[i], decoded[i]);
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse0() throws Throwable {
        DataImpl dataImpl = new DataImpl();
        doDecodeBooleanArrayTrueFalseOnlyTestImpl(dataImpl, true, 0, false);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.BOOL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse0Array32() throws Throwable {
        DataImpl dataImpl = new DataImpl();
        doDecodeBooleanArrayTrueFalseOnlyTestImpl(dataImpl, false, 0, false);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.BOOL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyFalse255() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), true, 255, false);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Array element count 255 is specified to be greater than the allowed 0-width-element limit for arrays (0)", e.getMessage());
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 255, false);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 256, false);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 384, false);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue0() throws Throwable {
        DataImpl dataImpl = new DataImpl();
        doDecodeBooleanArrayTrueFalseOnlyTestImpl(dataImpl, true, 0, true);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.BOOL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue0Array32() throws Throwable {
        DataImpl dataImpl = new DataImpl();
        doDecodeBooleanArrayTrueFalseOnlyTestImpl(dataImpl, false, 0, true);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.BOOL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeBooleanArrayExplicitlyTrue10() throws Throwable {
        try {
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), true, 10, true);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 10, true);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), true, 255, true);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 255, true);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 256, true);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeBooleanArrayTrueFalseOnlyTestImpl(new DataImpl(), false, 384, true);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    private void doDecodeBooleanArrayTrueFalseOnlyWhenAllowedTestImpl(boolean array8, final int count, final boolean type) throws Throwable {
        DataImpl dataImpl = new DataImpl();
        dataImpl.setZeroWidthElementLimit(count);

        doDecodeBooleanArrayTrueFalseOnlyTestImpl(dataImpl, array8, count, type);

        assertEquals("unexpected array length", count, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.BOOL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertTrue("Expected array", decoded.getClass().isArray());
        assertEquals("unexpected array length", count, decoded.length);

        for (int i = 0; i < count; ++i) {
            assertEquals("unexpected value", type, decoded[i]);
        }
    }

    private long doDecodeBooleanArrayTrueFalseOnlyTestImpl(final DataImpl dataImpl, final boolean array8, final int count, final boolean type) throws Exception {
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

        return dataImpl.decode(buffer);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only0() throws Throwable {
        DataImpl dataImpl = new DataImpl();
        doDecodeListArrayList0OnlyTestImpl(dataImpl, true, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.LIST, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only0Array32() throws Throwable {
        DataImpl dataImpl = new DataImpl();
        doDecodeListArrayList0OnlyTestImpl(dataImpl, false, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.LIST, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only254() throws Throwable {
        try {
            doDecodeListArrayList0OnlyTestImpl(new DataImpl(), true, 254);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeListArrayList0OnlyTestImpl(new DataImpl(), false, 254);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeListArrayList0OnlyTestImpl(new DataImpl(), true, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeListArrayList0OnlyTestImpl(new DataImpl(), false, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeListArrayList0OnlyTestImpl(new DataImpl(), false, 384);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeListArrayExplicitlyList0Only384WhenAllowed() throws Throwable {
        doDecodeListArrayList0OnlyWhenAllowedTestImpl(false, 384);
    }

    private void doDecodeListArrayList0OnlyWhenAllowedTestImpl(final boolean array8, final int count) throws Exception {
        DataImpl dataImpl = new DataImpl();
        dataImpl.setZeroWidthElementLimit(count);

        doDecodeListArrayList0OnlyTestImpl(dataImpl, array8, count);

        assertEquals("unexpected array length", count, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.LIST, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertTrue("Expected array", decoded.getClass().isArray());
        assertEquals("unexpected array length", count, decoded.length);

        for (int i = 0; i < count; ++i) {
            assertEquals("unexpected value", Collections.emptyList(), decoded[i]);
        }
    }

    private long doDecodeListArrayList0OnlyTestImpl(DataImpl dataImpl, final boolean array8, final int count) throws Exception {
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

        return dataImpl.decode(buffer);
    }

    @Test
    public void testDecodeNullArray0() throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        doDecodeNullArrayTestImpl(dataImpl, true, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array length", Data.DataType.NULL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeNullArray0Array32() throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        doDecodeNullArrayTestImpl(dataImpl, false, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.NULL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeNullArray255() throws Throwable {
        try {
            doDecodeNullArrayTestImpl(new DataImpl(), true, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeNullArrayTestImpl(new DataImpl(), false, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeNullArrayTestImpl(new DataImpl(), false, 256);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeNullArrayTestImpl(new DataImpl(), false, 384);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeNullArray384WhenAllowed() throws Throwable {
        doDecodeNullArrayWhenAllowedTestImpl(false, 384);
    }

    private void doDecodeNullArrayWhenAllowedTestImpl(boolean array8, final int count) throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        dataImpl.setZeroWidthElementLimit(count);
        doDecodeNullArrayTestImpl(dataImpl, array8, count);

        assertEquals("unexpected array length", count, dataImpl.getArray());
        assertEquals("unexpected array length", Data.DataType.NULL, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertTrue("Expected array", decoded.getClass().isArray());
        assertEquals("unexpected array length", count, decoded.length);
    }

    private long doDecodeNullArrayTestImpl(DataImpl dataImpl, final boolean array8, final int count) throws Exception {
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

        return dataImpl.decode(buffer);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only0() throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        doDecodeUIntArrayUInt0OnlyTestImpl(dataImpl, true, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.UINT, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only0Array32() throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        doDecodeUIntArrayUInt0OnlyTestImpl(dataImpl, false, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.UINT, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only255() throws Throwable {
        try {
            doDecodeUIntArrayUInt0OnlyTestImpl(new DataImpl(), true, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeUIntArrayUInt0OnlyTestImpl(new DataImpl(), false, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeUIntArrayUInt0OnlyTestImpl(new DataImpl(), false, 256);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
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
            doDecodeUIntArrayUInt0OnlyTestImpl(new DataImpl(), false, 384);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyUInt0Only384WhenAllowed() throws Throwable {
        doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(false, 384);
    }

    private long doDecodeUIntArrayUInt0OnlyTestImpl(final DataImpl dataImpl, final boolean array8, final int count) throws Exception {
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

        return dataImpl.decode(buffer);
    }


    private void doDecodeUIntArrayUInt0OnlyWhenAllowedTestImpl(final boolean array8, final int count) throws Exception {
        final DataImpl dataImpl = new DataImpl();
        dataImpl.setZeroWidthElementLimit(count);
        doDecodeUIntArrayUInt0OnlyTestImpl(dataImpl, array8, count);

        assertEquals("unexpected array length", count, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.UINT, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", count, decoded.length);

        for (int i = 0; i < count; i++) {
            assertEquals(decoded[i], UnsignedInteger.ZERO);
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only0() throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        doDecodeUIntArrayULong0OnlyTestImpl(dataImpl, true, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.ULONG, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only0Array32() throws Throwable {
        final DataImpl dataImpl = new DataImpl();
        doDecodeUIntArrayULong0OnlyTestImpl(dataImpl, false, 0);

        assertEquals("unexpected array length", 0, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.ULONG, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", 0, decoded.length);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only255() throws Throwable {
        try {
            doDecodeUIntArrayULong0OnlyTestImpl(new DataImpl(), true, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only255WhenAllowed() throws Throwable {
        doDecodeUIntArrayULong0OnlyWhenAllowedTestImpl(true, 255);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only255Array32() throws Throwable {
        try {
            doDecodeUIntArrayULong0OnlyTestImpl(new DataImpl(), false, 255);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only255Array32WhenAllowed() throws Throwable {
        doDecodeUIntArrayULong0OnlyWhenAllowedTestImpl(false, 255);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only256() throws Throwable {
        try {
            doDecodeUIntArrayULong0OnlyTestImpl(new DataImpl(), false, 256);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only256WhenAllowed() throws Throwable {
        doDecodeUIntArrayULong0OnlyWhenAllowedTestImpl(false, 256);
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only384() throws Throwable {
        try {
            doDecodeUIntArrayULong0OnlyTestImpl(new DataImpl(), false, 384);
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testDecodeUIntArrayExplicitlyULong0Only384WhenAllowed() throws Throwable {
        doDecodeUIntArrayULong0OnlyWhenAllowedTestImpl(false, 384);
    }

    private long doDecodeUIntArrayULong0OnlyTestImpl(final DataImpl dataImpl, final boolean array8, final int count) throws Exception {
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

        return dataImpl.decode(buffer);
    }


    private void doDecodeUIntArrayULong0OnlyWhenAllowedTestImpl(final boolean array8, final int count) throws Exception {
        final DataImpl dataImpl = new DataImpl();
        dataImpl.setZeroWidthElementLimit(count);
        doDecodeUIntArrayULong0OnlyTestImpl(dataImpl, array8, count);

        assertEquals("unexpected array length", count, dataImpl.getArray());
        assertEquals("unexpected array type", Data.DataType.ULONG, dataImpl.getArrayType());

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertEquals("unexpected array length", count, decoded.length);

        for (int i = 0; i < count; i++) {
            assertEquals(decoded[i], UnsignedLong.ZERO);
        }
    }

    @Test
    public void testDecodeArrayOfDescribedExplicitTrue() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.BOOLEAN_TRUE;
        final Boolean described = Boolean.TRUE;
        final byte arrayType = EncodingCodes.ARRAY32;

        final DataImpl dataImpl = new DataImpl();
        final Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedExplicitTrueArray8() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.BOOLEAN_TRUE;
        final Boolean described = Boolean.TRUE;
        final byte arrayType = EncodingCodes.ARRAY8;

        final DataImpl dataImpl = new DataImpl();
        final Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedExplicitFalse() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.BOOLEAN_FALSE;
        final Boolean described = Boolean.FALSE;
        final byte arrayType = EncodingCodes.ARRAY32;

        final DataImpl dataImpl = new DataImpl();
        final Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedULong0() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.ULONG0;
        final UnsignedLong described = UnsignedLong.ZERO;
        final byte arrayType = EncodingCodes.ARRAY32;

        final DataImpl dataImpl = new DataImpl();
        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedUInt0() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.UINT0;
        final UnsignedInteger described = UnsignedInteger.ZERO;
        final byte arrayType = EncodingCodes.ARRAY32;

        final DataImpl dataImpl = new DataImpl();
        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedNull() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.NULL;
        final Object described = null;
        final byte arrayType = EncodingCodes.ARRAY32;

        final DataImpl dataImpl = new DataImpl();
        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedList0() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.LIST0;
        final List<Integer> described = new ArrayList<Integer>();
        final byte arrayType = EncodingCodes.ARRAY32;

        final DataImpl dataImpl = new DataImpl();
        Object[] decoded = doDecodeArrayOfDescribedZeroWidthTestImpl(dataImpl, arrayType, describedPrimitiveConstructorCode);

        assertArrayOfDescribedTypeValues(decoded, described);
    }

    @Test
    public void testDecodeArrayOfDescribedNested() throws IOException {
        final byte describedPrimitiveConstructorCode = (byte) EncodingCodes.DESCRIBED_TYPE_INDICATOR;

        try {
            doDecodeArrayOfDescribedZeroWidthTestImpl(new DataImpl(), EncodingCodes.ARRAY32, describedPrimitiveConstructorCode);
            fail("Expected exception");
        } catch(IllegalArgumentException iae) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", iae.getMessage());
        }

        buffer.clear();

        try {
            doDecodeArrayOfDescribedZeroWidthTestImpl(new DataImpl(), EncodingCodes.ARRAY8, describedPrimitiveConstructorCode);
            fail("Expected exception");
        } catch(IllegalArgumentException iae) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", iae.getMessage());
        }
    }

    private Object[] doDecodeArrayOfDescribedZeroWidthTestImpl(final DataImpl dataImpl, final byte arrayTypeCode, final byte describedPrimitiveConstructorCode) {
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

        final int count = 2;
        if (arrayTypeCode == EncodingCodes.ARRAY32) {
            buffer.put(EncodingCodes.ARRAY32);
            buffer.putInt(4 + 1 + 1 + 1 + 10 + 1);  // Size
            buffer.putInt(count);  // Count
        } else {
            buffer.put(EncodingCodes.ARRAY8);
            buffer.put((byte) (1 + 1 + 1 + 1 + 10 + 1));  // Size
            buffer.put((byte) count);  // Count
        }
        buffer.put((byte) 0); // Described Type Indicator
        buffer.put(EncodingCodes.SYM8); // sym8 type code for symbolic descriptor
        buffer.put((byte) 10); //symbol length
        buffer.put("DESCRIPTOR".getBytes(StandardCharsets.US_ASCII));
        buffer.put(describedPrimitiveConstructorCode); // ulong constructor type-code for described primitive
        // We dont populate any value bytes as we are expecting 0 length element constructors (or described type indicator) to throw

        byte[] expectedEncoding = new byte[buffer.position()];

        buffer.flip();

        buffer.get(expectedEncoding);
        assertEquals("Should have drained the encoder buffer contents", 0, buffer.remaining());

        buffer.flip();

        // First verify that it throws with the depth limit default
        try {
            dataImpl.decode(buffer);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            // Expected
        }

        buffer.rewind();
        assertEquals("Should have expected buffer content remaining", expectedEncoding.length, buffer.remaining());

        // Now try again with permissive depth limit
        dataImpl.clear();
        dataImpl.setZeroWidthElementLimit(2);
        dataImpl.decode(buffer);

        assertEquals("unexpected array length", count, dataImpl.getArray());
        final DataType arrayType = dataImpl.getArrayType();
        switch(describedPrimitiveConstructorCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                assertEquals("unexpected array type", Data.DataType.BOOL, arrayType);
                break;
            case EncodingCodes.BOOLEAN_FALSE:
                assertEquals("unexpected array type", Data.DataType.BOOL, arrayType);
                break;
            case EncodingCodes.UINT0:
                assertEquals("unexpected array type", Data.DataType.UINT, arrayType);
                break;
            case EncodingCodes.ULONG0:
                assertEquals("unexpected array type", Data.DataType.ULONG, arrayType);
                break;
            case EncodingCodes.LIST0:
                assertEquals("unexpected array type", Data.DataType.LIST, arrayType);
                break;
            case EncodingCodes.NULL:
                assertEquals("unexpected array type", Data.DataType.NULL, arrayType);
                break;
            case EncodingCodes.DESCRIBED_TYPE_INDICATOR:
                assertEquals("unexpected array type", Data.DataType.DESCRIBED, arrayType);
                break;
            default:
                throw new IllegalStateException("Unexpected type code: " + arrayTypeCode);
        }

        Object[] decoded = dataImpl.getJavaArray();
        assertNotNull("Array should not be null", decoded);
        assertTrue("Expected array", decoded.getClass().isArray());
        assertEquals("unexpected array length", count, decoded.length);

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
    public void testDecodeArrayInteger() throws Exception {
        Object[] o = doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 26, 6);
        assertDecodedIntegerArray(o);

        o = doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 29, 6);
        assertDecodedIntegerArray(o);
    }

    private void assertDecodedIntegerArray(Object[] array) {
        assertNotNull(array);

        assertEquals(6, array.length);

        assertEquals(6, array[0]);
        assertEquals(5, array[1]);
        assertEquals(4, array[2]);
        assertEquals(3, array[3]);
        assertEquals(2, array[4]);
        assertEquals(1, array[5]);
    }

    @Test
    public void testDecodeArrayReturnsUnderflowWithSizeBeyondAvailable() throws Exception {
        doDecodeArrayReturnsUnderflowWithSizeBeyondAvailableTestImpl(false, 26 + 1, 6);

        doDecodeArrayReturnsUnderflowWithSizeBeyondAvailableTestImpl(true, 29 + 1, 6);
    }

    private void doDecodeArrayReturnsUnderflowWithSizeBeyondAvailableTestImpl(final boolean array32, final int size, final int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareIntegerArrayWithSpecificSizeCount(array32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(tmpBuf);

        assertEquals("Expected underflow return code", -4, result);
        assertEquals("No elements were expected", 0, dataImpl.size());

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(0, tmpBuf.position());
    }

    @Test
    public void testDecodeArrayThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Array size -1 cannot be negative", iae.getMessage());
        }
    }

    @Test
    public void testDecodeArrayThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 29, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Array element count -1 cannot be negative", iae.getMessage());
        }
    }

    @Test
    public void testDecodeArrayThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full Array, so the 'remaining' checks dont foul
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(false, 2, 6);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Array element count 6 is specified to be greater than the size of array (2)", iae.getMessage());
        }

        try {
            // Deliberately tries not to decode the full Array, so the 'remaining' checks dont foul
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 5, 6);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Array element count 6 is specified to be greater than the size of array (5)", iae.getMessage());
        }
    }

    private Object[] doDecodeIntegerArrayWithSpecificSizeCountTestImpl(final boolean array32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareIntegerArrayWithSpecificSizeCount(array32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final Object[] decoded;
        try
        {
             dataImpl.decode(tmpBuf);

             assertEquals("unexpected array length", count, dataImpl.getArray());
             assertEquals("unexpected array type", Data.DataType.INT, dataImpl.getArrayType());

             decoded = dataImpl.getJavaArray();
             assertNotNull("Array should not be null", decoded);
             assertEquals("unexpected array length", count, decoded.length);
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void prepareIntegerArrayWithSpecificSizeCount(final boolean array32, int size, int count, ByteBuffer tmpBuf) {
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
        } catch (IllegalArgumentException iae) {
            assertEquals("Array incorrectly encoded, left 4 bytes from size indicated 22", iae.getMessage());
        }

        try {
            // Deliberately pass size for full Array, but count for part Array...will try to decode count, underflow
            doDecodeIntegerArrayWithSpecificSizeCountTestImpl(true, 25, 5 - 1);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Array incorrectly encoded, left 4 bytes from size indicated 25", iae.getMessage());
        }
    }

    @Test
    public void testDecodeListInteger() throws Exception {
        List<Object> o = doDecodeIntegerListWithSpecificSizeCountTestImpl(false, 16, 3);
        assertDecodedIntegerList(o);

        o = doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 19, 3);
        assertDecodedIntegerList(o);
    }

    @Test
    public void testDecodeListReturnsUnderflowWithSizeBeyondAvailable() throws Exception {
        doDecodeListReturnsUnderflowWithSizeBeyondAvailableTestImpl(false, 17 + 1, 3);

        doDecodeListReturnsUnderflowWithSizeBeyondAvailableTestImpl(true, 20 + 1, 3);
    }

    private void doDecodeListReturnsUnderflowWithSizeBeyondAvailableTestImpl(final boolean array32, final int size, final int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareListIntegerWithSpecificSizeCount(array32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(tmpBuf);

        assertEquals("Expected underflow return code", -4, result);
        assertEquals("No elements were expected", 0, dataImpl.size());

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(0, tmpBuf.position());
    }

    @Test
    public void testDecodeListThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 3);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("List size -1 cannot be negative", iae.getMessage());
        }
    }

    @Test
    public void testDecodeListThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 19, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("List element count -1 cannot be negative", iae.getMessage());
        }
    }

    @Test
    public void testDecodeListThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeIntegerListWithSpecificSizeCountTestImpl(false, 11, 12);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("List element count 12 must be less than size (11)", iae.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeIntegerListWithSpecificSizeCountTestImpl(true, 13, 14);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("List element count 14 must be less than size (13)", iae.getMessage());
        }
    }

    private List<Object> doDecodeIntegerListWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareListIntegerWithSpecificSizeCount(list32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final List<Object> decoded;
        try
        {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.LIST, dataImpl.type());
            assertEquals("unexpected list length", count, dataImpl.getList());

            decoded = dataImpl.getJavaList();
            assertNotNull("List should not be null", decoded);
            assertEquals("unexpected list length", count, decoded.size());
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void prepareListIntegerWithSpecificSizeCount(final boolean list32, int size, int count, ByteBuffer tmpBuf) {
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
    }

    private void assertDecodedIntegerList(List<Object> list) {
        assertEquals(3, list.size());
        assertEquals(Integer.valueOf(3), list.get(0));
        assertEquals(Integer.valueOf(2), list.get(1));
        assertEquals(Integer.valueOf(1), list.get(2));
    }

    @Test
    public void testDecodeListOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow
            doDecodeListUnderflowOverflowTestImpl(false, 11, 3);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow
            doDecodeListUnderflowOverflowTestImpl(true, 14, 3);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeListUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow
            doDecodeListUnderflowOverflowTestImpl(false, 16, 2);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("List incorrectly encoded, left 5 bytes from size indicated 16", iae.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow
            doDecodeListUnderflowOverflowTestImpl(true, 19, 2);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("List incorrectly encoded, left 5 bytes from size indicated 19", iae.getMessage());
        }
    }

    private Object doDecodeListUnderflowOverflowTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareListIntegerWithSpecificSizeCount(list32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final List<Object> decoded;
        try
        {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.LIST, dataImpl.type());
            assertEquals("unexpected list length", count, dataImpl.getList());

            decoded = dataImpl.getJavaList();
            assertNotNull("List should not be null", decoded);
            assertEquals("unexpected list length", count, decoded.size());
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }

        return decoded;
    }

    @Test
    public void testDecodeMapOfInteger() throws Exception {
        Map<Object, Object> o = doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(false, 25, 6);
        assertDecodedMapOfInteger(o);

        o = doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28, 6);
        assertDecodedMapOfInteger(o);
    }

    @Test
    public void testDecodeMapReturnsUnderflowWithSizeBeyondAvailable() throws Exception {
        doDecodeMapReturnsUnderflowWithSizeBeyondAvailableTestImpl(false, 25 + 1, 3);

        doDecodeMapReturnsUnderflowWithSizeBeyondAvailableTestImpl(true, 28 + 1, 3);
    }

    private void doDecodeMapReturnsUnderflowWithSizeBeyondAvailableTestImpl(final boolean array32, final int size, final int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareMapIntegerWithSpecificSizeCount(array32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(tmpBuf);

        assertEquals("Expected underflow return code", -4, result);
        assertEquals("No elements were expected", 0, dataImpl.size());

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(0, tmpBuf.position());
    }

    @Test
    public void testDecodeMapThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map size -1 cannot be negative", iae.getMessage());
        }
    }

    @Test
    public void testDecodeMapThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map element count -1 cannot be negative", iae.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full map, so the 'remaining' checks dont foul
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 20, 22);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map element count 22 must be less than size (20)", iae.getMessage());
        }
    }

    @Test
    public void testDecodeMapThrowsWithOddCount() throws Exception {
        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(false, 25, 5);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map element count 5 cannot be odd", iae.getMessage());
        }

        try {
            doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(true, 28, 5);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map element count 5 cannot be odd", iae.getMessage());
        }
    }

    private Map<Object, Object> doDecodeMapOfIntegerWithSpecificSizeCountTestImpl(final boolean map32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareMapIntegerWithSpecificSizeCount(map32, size, count, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final Map<Object, Object> decoded;
        try
        {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.MAP, dataImpl.type());
            assertEquals("unexpected map count", count, dataImpl.getMap());

            decoded = dataImpl.getJavaMap();
            assertNotNull("Map should not be null", decoded);
            assertEquals("unexpected map size", count / 2 , decoded.size());
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void prepareMapIntegerWithSpecificSizeCount(final boolean map32, int size, int count, ByteBuffer tmpBuf) {
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
    }

    private void assertDecodedMapOfInteger(Map<Object, Object> map) {
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
            doDecodeMapUnderflowOverflowTestImpl(false, 11, 4);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part map, but count for full map...will try to decode count, overflow
            doDecodeMapUnderflowOverflowTestImpl(true, 14, 4);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeMapUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full map, but count for part map...will try to decode count, underflow
            doDecodeMapUnderflowOverflowTestImpl(false, 21, 2);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map incorrectly encoded, left 10 bytes from size indicated 21", iae.getMessage());
        }

        try {
            // Deliberately pass size for full map, but count for part map...will try to decode count, underflow
            doDecodeMapUnderflowOverflowTestImpl(true, 24, 2);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Map incorrectly encoded, left 10 bytes from size indicated 24", iae.getMessage());
        }
    }

    private Object doDecodeMapUnderflowOverflowTestImpl(final boolean map32, int size, int count) {
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

        DataImpl dataImpl = new DataImpl();

        final Map<Object, Object> decoded;
        try {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.MAP, dataImpl.type());
            assertEquals("unexpected map count", count, dataImpl.getMap());

            decoded = dataImpl.getJavaMap();
            assertNotNull("Map should not be null", decoded);
            assertEquals("unexpected map size", count / 2 , decoded.size());
        } finally {
            assertEquals(oldLimit, tmpBuf.limit());
        }

        return decoded;
    }

    @Test
    public void testDecodeBinary() throws Exception {
        final Binary expected = new Binary("54321".getBytes());

        Object o = doDecodeBinaryWithSpecificSizeTestImpl(false, 5);
        assertEquals(expected, o);

        o = doDecodeBinaryWithSpecificSizeTestImpl(true, 5);
        assertEquals(expected, o);
    }

    @Test
    public void testDecodeBinaryReturnsUnderflowWithSizeBeyondAvailable() throws Exception {
        doDecodeBinaryReturnsUnderflowWithSizeBeyondAvailableTestImpl(false, 5 + 1);

        doDecodeBinaryReturnsUnderflowWithSizeBeyondAvailableTestImpl(true, 5 + 1);
    }

    private void doDecodeBinaryReturnsUnderflowWithSizeBeyondAvailableTestImpl(final boolean vbin32, final int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareBinaryWithSpecificSize(vbin32, size, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(tmpBuf);

        assertEquals("Expected underflow return code", -4, result);
        assertEquals("No elements were expected", 0, dataImpl.size());

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(0, tmpBuf.position());
    }

    @Test
    public void testDecodeBinaryThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeBinaryWithSpecificSizeTestImpl(true, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Binary size -1 cannot be negative", iae.getMessage());
        }
    }

    private Object doDecodeBinaryWithSpecificSizeTestImpl(final boolean vbin32, int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareBinaryWithSpecificSize(vbin32, size, tmpBuf);
        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final Binary decoded;
        try
        {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.BINARY, dataImpl.type());

            decoded = dataImpl.getBinary();
            assertNotNull("Binary should not be null", decoded);
            assertEquals("unexpected binary size", size, decoded.getLength());
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void prepareBinaryWithSpecificSize(final boolean vbin32, int size, ByteBuffer tmpBuf) {
        if (vbin32) {
            tmpBuf.put((byte) (byte) 0xB0); // vbin32 primitive type constructor
            tmpBuf.putInt(size); // size
        } else {
            assertTrue(size < 256);
            tmpBuf.put((byte) (byte) 0xA0); // vbin8 primitive type constructor
            tmpBuf.put((byte) size); // size
        }
        tmpBuf.put((byte) 0x35); // "5"
        tmpBuf.put((byte) 0x34); // "4"
        tmpBuf.put((byte) 0x33); // "3"
        tmpBuf.put((byte) 0x32); // "2"
        tmpBuf.put((byte) 0x31); // "1"

        tmpBuf.flip();
    }

    @Test
    public void testDecodeString() throws Exception {
        Object o = doDecodeStringWithSpecificSizeTestImpl(false, 5);
        assertEquals("54321", o);

        o = doDecodeStringWithSpecificSizeTestImpl(true, 5);
        assertEquals("54321", o);
    }

    @Test
    public void testDecodeStringReturnsUnderflowWithSizeBeyondAvailable() throws Exception {
        doDecodeStringReturnsUnderflowWithSizeBeyondAvailableTestImpl(false, 5 + 1);

        doDecodeStringReturnsUnderflowWithSizeBeyondAvailableTestImpl(true, 5 + 1);
    }

    private void doDecodeStringReturnsUnderflowWithSizeBeyondAvailableTestImpl(final boolean str32, final int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareStringWithSpecificSize(str32, size, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(tmpBuf);

        assertEquals("Expected underflow return code", -4, result);
        assertEquals("No elements were expected", 0, dataImpl.size());

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(0, tmpBuf.position());
    }

    @Test
    public void testDecodeStringThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeStringWithSpecificSizeTestImpl(true, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("String size -1 cannot be negative", iae.getMessage());
        }
    }

    private Object doDecodeStringWithSpecificSizeTestImpl(final boolean str32, int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareStringWithSpecificSize(str32, size, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final String decoded;
        try
        {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.STRING, dataImpl.type());

            decoded = dataImpl.getString();
            assertNotNull("String should not be null", decoded);
            assertEquals("unexpected string size", size , decoded.length());
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void prepareStringWithSpecificSize(final boolean str32, int size, ByteBuffer tmpBuf) {
        if (str32) {
            tmpBuf.put((byte) (byte) 0xB1); // str32 primitive type constructor
            tmpBuf.putInt(size); // size
        } else {
            assertTrue(size < 256);
            tmpBuf.put((byte) (byte) 0xA1); // str8 primitive type constructor
            tmpBuf.put((byte) size); // size
        }
        tmpBuf.put((byte) 0x35); // "5"
        tmpBuf.put((byte) 0x34); // "4"
        tmpBuf.put((byte) 0x33); // "3"
        tmpBuf.put((byte) 0x32); // "2"
        tmpBuf.put((byte) 0x31); // "1"

        tmpBuf.flip();
    }

    @Test
    public void testDecodeSymbol() throws Exception {
        Object o = doDecodeSymbolWithSpecificSizeTestImpl(false, 5);
        assertEquals(Symbol.valueOf("54321"), o);

        o = doDecodeSymbolWithSpecificSizeTestImpl(true, 5);
        assertEquals(Symbol.valueOf("54321"), o);
    }

    @Test
    public void testDecodeSymbolReturnsUnderflowWithSizeBeyondAvailable() throws Exception {
        doDecodeSymbolReturnsUnderflowWithSizeBeyondAvailableTestImpl(false, 5 + 1);

        doDecodeSymbolReturnsUnderflowWithSizeBeyondAvailableTestImpl(true, 5 + 1);
    }

    private void doDecodeSymbolReturnsUnderflowWithSizeBeyondAvailableTestImpl(final boolean sym32, final int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareSymbolWithSpecificSize(sym32, size, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(tmpBuf);

        assertEquals("Expected underflow return code", -4, result);
        assertEquals("No elements were expected", 0, dataImpl.size());

        assertEquals(oldLimit, tmpBuf.limit());
        assertEquals(0, tmpBuf.position());
    }

    @Test
    public void testDecodeSymbolThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeSymbolWithSpecificSizeTestImpl(true, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("Symbol size -1 cannot be negative", iae.getMessage());
        }
    }

    private Object doDecodeSymbolWithSpecificSizeTestImpl(final boolean sym32, int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        prepareSymbolWithSpecificSize(sym32, size, tmpBuf);

        int oldLimit = tmpBuf.limit();

        DataImpl dataImpl = new DataImpl();

        final Symbol decoded;
        try
        {
            dataImpl.decode(tmpBuf);

            assertEquals("unexpected type", Data.DataType.SYMBOL, dataImpl.type());

            decoded = dataImpl.getSymbol();
            assertNotNull("String should not be null", decoded);
            assertEquals("unexpected string size", size , decoded.length());
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    private void prepareSymbolWithSpecificSize(final boolean sym32, int size, ByteBuffer tmpBuf) {
        if (sym32) {
            tmpBuf.put((byte) (byte) 0xB3); // sym32 primitive type constructor
            tmpBuf.putInt(size); // size
        } else {
            assertTrue(size < 256);
            tmpBuf.put((byte) (byte) 0xA3); // sym8 primitive type constructor
            tmpBuf.put((byte) size); // size
        }
        tmpBuf.put((byte) 0x35); // "5"
        tmpBuf.put((byte) 0x34); // "4"
        tmpBuf.put((byte) 0x33); // "3"
        tmpBuf.put((byte) 0x32); // "2"
        tmpBuf.put((byte) 0x31); // "1"

        tmpBuf.flip();
    }

    @Test
    public void testDescribedTypeValueBeingDescribedTypeThrows() throws Exception {
        // DescribedTypes constructors describe a primitive AMQP type, not another DescribedType
        final byte[] descriptor1bytes = "UNKNOWN1".getBytes(StandardCharsets.US_ASCII);
        assertTrue(descriptor1bytes.length <= 255);

        final byte[] descriptor2bytes = "UNKNOWN2".getBytes(StandardCharsets.US_ASCII);
        assertTrue(descriptor2bytes.length <= 255);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptor1bytes.length); // descriptor1 length
        buffer.put(descriptor1bytes); // descriptor value
        buffer.put((byte) 0x00); // Described value type code. Another DescribedType (instead of the expected primitive type code)
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptor2bytes.length); // descriptor2 length
        buffer.put(descriptor1bytes); // descriptor2 value
        buffer.put((byte) 0x45); // list0 primitive type constructor

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        try {
            dataImpl.decode(buffer);

            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", iae.getMessage());
        }
    }

    @Test
    public void testDescribedTypeDescriptorBeingDescribedTypeThrows() throws Exception {
        // Descriptors except ulong and symbol are reserved, none are used
        final byte[] descriptor1bytes = "UNKNOWN1".getBytes(StandardCharsets.US_ASCII);
        assertTrue(descriptor1bytes.length <= 255);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) 0x00); // Nested DescribedType as the descriptor. Reserved.
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptor1bytes.length); // descriptor length
        buffer.put(descriptor1bytes); // descriptor value
        buffer.put((byte) 0x45); // list0 primitive type constructor
        buffer.put((byte) 0x45); // list0 primitive type constructor (for outer DescribedType value)

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        try {
            dataImpl.decode(buffer);

            fail("Expected exception");
          } catch (IllegalArgumentException iae) {
          assertEquals("Reserved non-ulong/non-symbol described-type descriptors not supported by this decoder", iae.getMessage());
        }
    }

    @Test
    public void testDescribedTypeDescriptorExceedingAvailableBytesReturnsOverflow() throws Exception {
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM32); // Symbol descriptor
        buffer.putInt(10); // Descriptor length. Exceeds available bytes.

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(buffer);

        assertEquals(-4, result);
    }

    @Test
    public void testDescribedTypeDescribedExceedingAvailableBytesReturnsOverflow() throws Exception {
        final byte[] descriptorBytes = "UNKNOWN1".getBytes(StandardCharsets.US_ASCII);
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM32); // Symbol descriptor
        buffer.putInt(descriptorBytes.length); // Descriptor length.
        buffer.put(descriptorBytes);
        buffer.put(EncodingCodes.STR32); // String type
        buffer.putInt(10); // String length. Exceeds available bytes.

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        long result = dataImpl.decode(buffer);

        assertEquals(-4, result);
    }

    @Test
    public void testDescribedTypeConstructorDescriptorExceedingIntRangeThrows() throws Exception {
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM32); // Symbol descriptor
        buffer.putInt(Integer.MAX_VALUE); // Descriptor length. Add descriptor type code and size int and it exceeds int range.

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        try {
            dataImpl.decode(buffer);

            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("constructor size cannot be negative: -2147483645", iae.getMessage());
        }
    }

    @Test
    public void testDescribedTypeConstructorDescribedExceedingIntRangeThrows() throws Exception {
        final byte[] descriptorBytes = "UNKNOWN1".getBytes(StandardCharsets.US_ASCII);
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM32); // Symbol descriptor
        buffer.putInt(descriptorBytes.length); // Descriptor length.
        buffer.put(descriptorBytes);
        buffer.put(EncodingCodes.STR32); // String type
        buffer.putInt(Integer.MAX_VALUE); // Described string length. Add type code and size int and its constructor exceeds int.

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        try {
            dataImpl.decode(buffer);

            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("constructor size cannot be negative: -2147483645", iae.getMessage());
        }
    }

    @Test
    public void testDescribedTypeConstructorComboExceedingIntRangeThrows() throws Exception {
        final byte[] descriptorBytes = "UNKNOWN1".getBytes(StandardCharsets.US_ASCII);
        buffer.put((byte) 0x00); // DescribedType

        buffer.put(EncodingCodes.SYM32); // Symbol descriptor
        buffer.putInt(descriptorBytes.length); // Descriptor length.
        buffer.put(descriptorBytes);
        buffer.put(EncodingCodes.STR32); // String type
        final int previousConstructorBytesLength = 1 + descriptorBytes.length + 4 + 1; // Excludes the initial 0x00
        buffer.putInt((Integer.MAX_VALUE - previousConstructorBytesLength) + 1 ); // Described string length.
        // Add type codes and size int, and the combination of descriptor + described makes overall DescribedTypeConstructor exceed int.

        buffer.flip();

        DataImpl dataImpl = new DataImpl();

        try {
            dataImpl.decode(buffer);

            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("DescribedTypeConstructor total size cannot exceed 2147483647 but was: 2147483652", iae.getMessage());
        }
    }

    @Test
    public void testDecodeAmqpValueContainingDescribedType() throws IOException {
        final UnknownDescribedType described = new UnknownDescribedType(Symbol.valueOf("descriptor"), "testDecodeAmqpValueContainingDescribedType");

        encoder.writeObject(new AmqpValue(described));

        buffer.flip();

        DataImpl dataImpl = new DataImpl();
        dataImpl.decode(buffer);

        assertEquals("unexpected type", Data.DataType.DESCRIBED, dataImpl.type());
        final DescribedType result = dataImpl.getDescribedType();

        assertNotNull(result);

        final UnsignedLong amqpValueDescriptor = UnsignedLong.valueOf(0x0000000000000077L);

        assertEquals("unexpected descriptor", amqpValueDescriptor, result.getDescriptor());
        final DescribedType decodedDescribed = (DescribedType) result.getDescribed();

        assertEquals(described.getDescriptor(), decodedDescribed.getDescriptor());
        assertEquals(described.getDescribed(), decodedDescribed.getDescribed());
    }

    @Test
    public void testDecodeAmqpValueContainingDescribedTypeNested() throws IOException {
        final UnknownDescribedType nested = new UnknownDescribedType(Symbol.valueOf("descriptor2"), "nested");
        final UnknownDescribedType described = new UnknownDescribedType(Symbol.valueOf("descriptor"), nested);

        encoder.writeObject(new AmqpValue(described));

        buffer.flip();

        DataImpl dataImpl = new DataImpl();
        try {
            dataImpl.decode(buffer);
            fail("Expected exception");
        } catch(IllegalArgumentException iae) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", iae.getMessage());
            //expected
        }
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

        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(depthLimit);

        try {
            dataImpl.decode(buffer);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 10", iae.getMessage());
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

        dataImpl.clear();

        dataImpl.decode(buffer);

        assertTrue(dataImpl.getJavaList() instanceof List);
    }

    @Test
    public void testDecodingOfDeeplyNestedListOfList32andList0() throws Exception {
        doDecodingOfDeeplyNestedListOfListandList0TestImpl(true, 10);
    }

    @Test
    public void testDecodingOfDeeplyNestedListOfList8andList0() throws Exception {
        doDecodingOfDeeplyNestedListOfListandList0TestImpl(false, 10);
    }

    private void doDecodingOfDeeplyNestedListOfListandList0TestImpl(final boolean list32, final int depth) {
        ByteBuffer tmpBuf = buildNestedListsBuffer(depth, list32);

        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(depth - 1);

        try {
            dataImpl.decode(tmpBuf);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: " + (depth - 1), iae.getMessage());
            //expected
        }

        dataImpl.clear();
        tmpBuf.clear();

        dataImpl.setMaxDecodeDepth(depth);

        dataImpl.decode(tmpBuf);

        assertTrue(dataImpl.getJavaList() instanceof List);
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
        buffer.flip();

        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(depthLimit);

        try {
            dataImpl.decode(buffer);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 10", iae.getMessage());
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

        dataImpl.clear();

        dataImpl.decode(buffer);

        assertTrue(dataImpl.getJavaMap() instanceof Map);
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

        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(depth - 1);

        try {
            dataImpl.decode(tmpBuf);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: " + (depth - 1), iae.getMessage());
            //expected
        }

        dataImpl.clear();
        tmpBuf.clear();

        dataImpl.setMaxDecodeDepth(depth);

        dataImpl.decode(tmpBuf);

        assertTrue(dataImpl.getJavaMap() instanceof Map);
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

    @Test
    public void testDecodeArrayFailsIfDepthLimitExceeded() throws Exception {
        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(1);
        final int depth = 2;
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
            dataImpl.decode(buffer);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", iae.getMessage());
        }

        dataImpl.clear();
        dataImpl.setMaxDecodeDepth(depth);
        buffer.reset();

        try {
            dataImpl.decode(buffer);

            assertEquals("unexpected array size", width, dataImpl.getArray());
            assertEquals("unexpected array type", Data.DataType.ARRAY, dataImpl.getArrayType());

            final Object[] decoded = dataImpl.getJavaArray();
            assertNotNull("Array should not be null", decoded);
            assertEquals("unexpected array size", width, decoded.length);

            for (int i = 0; i < width; ++i) {
                assertNotNull(decoded[i]);
                assertTrue(decoded[i].getClass().isArray());
                assertEquals(size, ((Object[]) decoded[i]).length);

                assertArrayEquals(source[i], ((Object[]) decoded[i]));
            }
        } catch (Exception e) {
            fail("Should be able to read the array after depth configuration change");
        }
    }

    @Test
    public void testDecodeArrayOfListsOfListsFailsIfDepthLimitExceeded() throws Exception {
        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(1);

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
            dataImpl.decode(buffer);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", iae.getMessage());
        }

        buffer.reset();

        dataImpl.clear();
        dataImpl.setMaxDecodeDepth(2);

        try {
            dataImpl.decode(buffer);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 2", iae.getMessage());
        }

        buffer.reset();

        dataImpl.clear();
        dataImpl.setMaxDecodeDepth(3);

        try {
            dataImpl.decode(buffer);

            assertEquals("unexpected array size", 2, dataImpl.getArray());
            assertEquals("unexpected array type", Data.DataType.LIST, dataImpl.getArrayType());

            final Object[] decoded = dataImpl.getJavaArray();
            assertNotNull("Array should not be null", decoded);
            assertEquals("unexpected array size", 2, decoded.length);
        } catch (Exception e) {
            fail("Should be able to read the array after depth configuration change");
        }
    }

    @Test
    public void testDecodeArrayOfListsOfMapsFailsIfDepthLimitExceeded() throws Exception {
        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(1);

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
            dataImpl.decode(buffer);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", iae.getMessage());
        }

        buffer.reset();

        dataImpl.clear();
        dataImpl.setMaxDecodeDepth(2);

        try {
            dataImpl.decode(buffer);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 2", iae.getMessage());
        }

        buffer.reset();

        dataImpl.clear();
        dataImpl.setMaxDecodeDepth(3);

        try {
            dataImpl.decode(buffer);

            assertEquals("unexpected array size", 2, dataImpl.getArray());
            assertEquals("unexpected array type", Data.DataType.LIST, dataImpl.getArrayType());

            final Object[] decoded = dataImpl.getJavaArray();
            assertNotNull("Array should not be null", decoded);
            assertEquals("unexpected array size", 2, decoded.length);
        } catch (Exception e) {
            fail("Should be able to read the array after depth configuration change");
        }
    }

    @Test
    public void testDecodingOfDeeplyNestedArrayOfArray32() throws Exception {
        doDecodingOfDeeplyNestedArrayOfArrayTestImpl(true, 10);
    }

    @Test
    public void testDecodingOfDeeplyNestedArrayOfArray8() throws Exception {
        doDecodingOfDeeplyNestedArrayOfArrayTestImpl(false, 10);
    }

    private void doDecodingOfDeeplyNestedArrayOfArrayTestImpl(final boolean array32, final int depth) {
        ByteBuffer tmpBuf = buildNestedArraysBuffer(depth, array32);

        DataImpl dataImpl = new DataImpl();
        dataImpl.setMaxDecodeDepth(depth - 1);

        try {
            dataImpl.decode(tmpBuf);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: " + (depth - 1), iae.getMessage());
            //expected
        }

        dataImpl.setMaxDecodeDepth(depth);
        dataImpl.clear();
        tmpBuf.clear();

        dataImpl.decode(tmpBuf);

        assertTrue(dataImpl.getJavaArray() instanceof Object[]);
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

    @Test
    public void testDecodeAmqpValueContainingAmqpValueNested() throws IOException {
        final String valueString = "testDecodeAmqpValueContainingAmqpValueNested";
        final AmqpValue nested2 = new AmqpValue(valueString);
        final AmqpValue nested1 = new AmqpValue(nested2);

        encoder.writeObject(new AmqpValue(nested1));

        buffer.flip();
        int limit = buffer.limit();

        DataImpl dataImpl = new DataImpl();
        // We count amqp-value containing another described-type as having depth, so this is depth 2
        dataImpl.setMaxDecodeDepth(1);
        try {
            dataImpl.decode(buffer);
            fail("Expected exception");
        } catch (IllegalArgumentException iae) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", iae.getMessage());
        }

        buffer.clear();
        buffer.limit(limit);

        // Try again with permissive depth limit
        dataImpl.clear();
        dataImpl.setMaxDecodeDepth(2);
        dataImpl.decode(buffer);

        assertEquals("unexpected type", Data.DataType.DESCRIBED, dataImpl.type());
        final DescribedType result = dataImpl.getDescribedType();

        assertNotNull(result);
        final UnsignedLong amqpValueDescriptor = UnsignedLong.valueOf(0x0000000000000077L);
        assertEquals("unexpected descriptor", amqpValueDescriptor, result.getDescriptor());
        final DescribedType nestedDescribed1 = (DescribedType) result.getDescribed();
        assertEquals(amqpValueDescriptor, nestedDescribed1.getDescriptor());
        final DescribedType nestedDescribed2 = (DescribedType) nestedDescribed1.getDescribed();
        assertEquals(amqpValueDescriptor, nestedDescribed2.getDescriptor());
        assertEquals(valueString, nestedDescribed2.getDescribed());
    }
}
