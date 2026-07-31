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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;

/**
 * Tests the handling of UnknownDescribedType instances.
 */
public class UnknownDescribedTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeUnknownDescribedType() throws Exception {
        encoder.writeObject(NoLocalType.NO_LOCAL);

        buffer.clear();

        Object result = decoder.readObject();
        assertTrue(result instanceof DescribedType);
        DescribedType resultTye = (DescribedType) result;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testDecodeSmallSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnknownDescribedTypeInList() throws IOException {
        List<Object> listOfUnkowns = new ArrayList<>();

        listOfUnkowns.add(NoLocalType.NO_LOCAL);

        encoder.writeList(listOfUnkowns);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof List);

        final List<Object> decodedList = (List<Object>) result;
        assertEquals(1, decodedList.size());

        final Object listEntry = decodedList.get(0);
        assertTrue(listEntry instanceof DescribedType);

        DescribedType resultTye = (DescribedType) listEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnknownDescribedTypeInMap() throws IOException {
        Map<Object, Object> mapOfUnknowns = new HashMap<>();

        mapOfUnknowns.put(NoLocalType.NO_LOCAL.getDescriptor(), NoLocalType.NO_LOCAL);

        encoder.writeMap(mapOfUnknowns);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof Map);

        final Map<Object, Object> decodedMap = (Map<Object, Object>) result;
        assertEquals(1, decodedMap.size());

        final Object mapEntry = decodedMap.get(NoLocalType.NO_LOCAL.getDescriptor());
        assertTrue(mapEntry instanceof DescribedType);

        DescribedType resultTye = (DescribedType) mapEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testReadConstructorUnknownDescribedType() throws Exception {
        encoder.writeObject(NoLocalType.NO_LOCAL);

        buffer.flip();

        TypeConstructor<?> result = decoder.readConstructor();

        assertTrue(result instanceof DynamicTypeConstructor);
        DynamicTypeConstructor c1 = (DynamicTypeConstructor) result;

        assertTrue(buffer.remaining() > 0);

        buffer.flip();

        result = decoder.readConstructor();

        assertTrue(result instanceof DynamicTypeConstructor);
        DynamicTypeConstructor c2 = (DynamicTypeConstructor) result;

        assertSame(c1.getDescribedTypeConstructor(), c2.getDescribedTypeConstructor());
        assertSame(c1.getUnderlyingEncoding(), c2.getUnderlyingEncoding());
    }

    @Test
    public void testSymbolDescriptorConstructorReuse() throws Exception {

        final int limit = 16;
        int attempts = doSymbolDescriptorReuseTestImpl(limit, "UNKNOWN");
        assertTrue("Attempts not as expected: " + attempts, attempts > 5);
        assertTrue("Attempts not as expected: " + attempts, attempts <= limit);
    }

    @Test
    public void testSymbolDescriptorConstructorReuseLong() throws Exception {

        final int limit = 16;
        final StringBuilder descriptorPrefix = new StringBuilder("UNKNOWN");
        final String descriptorPrefixPad = "123456789_";
        for (int i = 1; i <= 6; i++)
        {
            descriptorPrefix.append(descriptorPrefixPad);
        }
        assertTrue(descriptorPrefix.length() > 64);

        int attempts = doSymbolDescriptorReuseTestImpl(limit, descriptorPrefix.toString());

        assertEquals("Expected no reuse", 0, attempts);
    }

    private int doSymbolDescriptorReuseTestImpl(final int limit, String descriptorPrefix) {
        boolean notSame = false;
        int attempts = 0;
        for (; attempts < limit + 5; attempts++)
        {
            final String descriptorValue = descriptorPrefix + attempts;
            final byte[] bytes = descriptorValue.getBytes(StandardCharsets.US_ASCII);
            assertTrue(bytes.length < 128);
            buffer.clear();

            buffer.put((byte) 0x00); // DescribedType
            buffer.put(EncodingCodes.SYM8); // Symbol
            buffer.put((byte) bytes.length); // descriptor length
            buffer.put(bytes); // descriptor value
            buffer.put((byte) 0x45); // list0 primitive type constructor

            buffer.flip();

            TypeConstructor<?> result = decoder.readConstructor();

            assertTrue(result instanceof DynamicTypeConstructor);
            DynamicTypeConstructor c1 = (DynamicTypeConstructor) result;

            buffer.flip();

            result = decoder.readConstructor();

            assertTrue(result instanceof DynamicTypeConstructor);
            DynamicTypeConstructor c2 = (DynamicTypeConstructor) result;

            if (c1.getDescribedTypeConstructor() != c2.getDescribedTypeConstructor())
            {
                notSame = true;
                break;
            }
        }

        assertTrue(notSame);

        return attempts;
    }

    @Test
    public void testUnknownDescribedTypeDuringSasl() throws IOException {
        final String descriptorValue = "UNKNOWN";
        final byte[] bytes = descriptorValue.getBytes(StandardCharsets.US_ASCII);
        assertTrue(bytes.length <= 255);

        DecoderImpl myDecoder = new DecoderImpl(true);
        new EncoderImpl(myDecoder, true); // Needed for type registration

        myDecoder.setByteBuffer(buffer);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) bytes.length); // descriptor length
        buffer.put(bytes); // descriptor value
        buffer.put((byte) 0x45); // list0 primitive type constructor

        buffer.flip();

        try {
            myDecoder.readObject();
            fail("Expected exception");
         } catch (DecodeException de) {
             assertNotNull(de.getMessage());
             assertEquals("Cannot decode unknown described types during SASL", de.getMessage());
         }
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

        try {
            decoder.readObject();
            fail("Expected exception");
         } catch (DecodeException de) {
             assertNotNull(de.getMessage());
             assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", de.getMessage());
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

        try {
            decoder.readObject();
            fail("Expected exception");
         } catch (DecodeException de) {
             assertNotNull(de.getMessage());
             assertEquals("Reserved non-ulong/non-symbol described-type descriptors not supported by this decoder", de.getMessage());
         }
    }

    private void doTestDecodeUnknownDescribedTypeSeries(int size) throws IOException {
        for (int i = 0; i < size; ++i) {
            encoder.writeObject(NoLocalType.NO_LOCAL);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof DescribedType);

            DescribedType resultTye = (DescribedType) result;
            assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
        }
    }
}
