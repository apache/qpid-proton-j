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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.codec.Data;
import org.junit.Test;

public class DataImplTest
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
}
