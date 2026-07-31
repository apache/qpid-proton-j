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
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.Test;

/**
 * Test for decoder of the AmqpValue type.
 */
public class AmqpValueTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeAmqpValueString() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue("test"));
    }

    @Test
    public void testDecodeAmqpValueNull() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue(null));
    }

    @Test
    public void testDecodeAmqpValueUUID() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue(UUID.randomUUID()));
    }

    @Test
    public void testDecodeSmallSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(SMALL_SIZE, new AmqpValue("test"));
    }

    @Test
    public void testDecodeLargeSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(LARGE_SIZE, new AmqpValue("test"));
    }

    private void doTestDecodeAmqpValueSeries(int size, AmqpValue value) throws IOException {

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(value);
        }

        buffer.flip();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof AmqpValue);

            AmqpValue decoded = (AmqpValue) result;

            assertEquals(value.getValue(), decoded.getValue());
        }
    }

    @Test
    public void testDecodeAmqpValueContainingDescribedType() throws IOException {
        final String descriptorValue = "UNKNOWN";
        final byte[] descriptorBytes = descriptorValue.getBytes(StandardCharsets.US_ASCII);
        final String describedString = "testDecodeAmqpValueContainingDescribedType";
        final byte[] describedStringBytes = describedString.getBytes(StandardCharsets.UTF_8);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptorBytes.length); // descriptor length
        buffer.put(descriptorBytes); // descriptor value
        buffer.put(EncodingCodes.STR8); // String
        buffer.put((byte) describedStringBytes.length); // string length
        buffer.put(describedStringBytes); // string value

        buffer.flip();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof AmqpValue);

        AmqpValue decoded = (AmqpValue) result;

        assertTrue(decoded.getValue() instanceof DescribedType);
        final DescribedType decodedDescribed = (DescribedType) decoded.getValue();
        assertEquals(Symbol.valueOf(descriptorValue), decodedDescribed.getDescriptor());
        assertEquals(describedString, decodedDescribed.getDescribed());
    }

    @Test
    public void testDecodeAmqpValueContainingDescribedTypeNested() throws IOException {
        final String descriptor1Value = "UNKNOWN1";
        final byte[] descriptor1Bytes = descriptor1Value.getBytes(StandardCharsets.US_ASCII);
        final String descriptor2Value = "UNKNOWN2";
        final byte[] descriptor2Bytes = descriptor2Value.getBytes(StandardCharsets.US_ASCII);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptor1Bytes.length); // descriptor length
        buffer.put(descriptor1Bytes); // descriptor value
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptor2Bytes.length); // descriptor2 length
        buffer.put(descriptor2Bytes); // descriptor2 value
        buffer.put((byte) 0x45); // list0 primitive type constructor

        buffer.flip();

        try {
            decoder.readObject();
            fail("Expected exception");
        } catch(DecodeException de) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", de.getMessage());
            //expected
        }
    }

    @Test
    public void testDecodeAmqpValueContainingDescribedTypeNestedAmqpValue() throws IOException {
        final String descriptorValue = "UNKNOWN1";
        final byte[] descriptorBytes = descriptorValue.getBytes(StandardCharsets.US_ASCII);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SYM8); // Symbol
        buffer.put((byte) descriptorBytes.length); // descriptor length
        buffer.put(descriptorBytes); // descriptor value
        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0x45); // list0 primitive type constructor

        buffer.flip();

        try {
            decoder.readObject();
            fail("Expected exception");
        } catch(DecodeException de) {
            assertEquals("DescribedType value encoding cannot be another DescribedType, must be a primitive type", de.getMessage());
            //expected
        }
    }

    @Test
    public void testDecodeAmqpValueContainingAmqpValueNested() throws IOException {
        final String describedString = "testDecodeAmqpValueContainingAmqpValueNested";
        final byte[] describedStringBytes = describedString.getBytes(StandardCharsets.UTF_8);

        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0x00); // DescribedType
        buffer.put((byte) EncodingCodes.SMALLULONG); // small-ulong constructor
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put(EncodingCodes.STR8); // String
        buffer.put((byte) describedStringBytes.length); // string length
        buffer.put(describedStringBytes); // string value

        buffer.flip();
        int limit = buffer.limit();

        // We count amqp-value containing another described-type as having depth, so this is depth 2
        decoder.setMaxDecodeDepth(1);
        try {
            decoder.readObject();
            fail("Expected exception");
        } catch(DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 1", de.getMessage());
            //expected
        }

        buffer.clear();
        buffer.limit(limit);

        // Try again with permissive depth limit
        decoder.setMaxDecodeDepth(2);
        Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof AmqpValue);
        final Object resultNested1 = ((AmqpValue)result).getValue();
        assertTrue(resultNested1 instanceof AmqpValue);
        final Object resultNested2 = ((AmqpValue)resultNested1).getValue();
        assertTrue(resultNested2 instanceof AmqpValue);
        assertEquals(describedString, ((AmqpValue) resultNested2).getValue());
    }
}
