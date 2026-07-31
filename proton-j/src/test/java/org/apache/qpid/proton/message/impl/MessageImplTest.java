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
package org.apache.qpid.proton.message.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer.ByteBufferWrapper;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageDecodeOptions;
import org.junit.Test;

public class MessageImplTest
{
    private static final long DATA_SECTION_ULONG_DESCRIPTOR = 0x0000000000000075L;

    @Test
    public void testEncodeOfMessageWithSmallDataBodyOnly()
    {
        doMessageEncodingWithDataBodySectionTestImpl(5);
    }

    @Test
    public void testEncodeOfMessageWithLargerDataBodyOnly()
    {
        doMessageEncodingWithDataBodySectionTestImpl(1024);
    }

    void doMessageEncodingWithDataBodySectionTestImpl(int bytesLength)
    {
        byte[] bytes = generateByteArray(bytesLength);

        byte[] expectedBytes = generateExpectedDataSectionBytes(bytes);
        byte[] encodedBytes = new byte[expectedBytes.length];

        Message msg = Message.Factory.create();
        msg.setBody(new Data(new Binary(bytes)));

        int encodedLength = msg.encode(encodedBytes, 0, encodedBytes.length);

        assertArrayEquals("Encoded bytes do not match expectation", expectedBytes, encodedBytes);
        assertEquals("Encoded length different than expected length", encodedLength, encodedBytes.length);
    }

    @Test
    public void testEncodeOfMessageWithSmallDataBodyOnlyUsingWritableBuffer()
    {
        doMessageEncodingWithDataBodySectionTestImpl(5);
    }

    @Test
    public void testEncodeOfMessageWithLargerDataBodyOnlyUsingWritableBuffer()
    {
        doMessageEncodingWithDataBodySectionTestImpl(1024);
    }

    void doMessageEncodingWithDataBodySectionTestImplUsingWritableBuffer(int bytesLength)
    {
        byte[] bytes = generateByteArray(bytesLength);

        byte[] expectedBytes = generateExpectedDataSectionBytes(bytes);
        ByteBufferWrapper encodedBytes = WritableBuffer.ByteBufferWrapper.allocate(expectedBytes.length);

        Message msg = Message.Factory.create();
        msg.setBody(new Data(new Binary(bytes)));

        int encodedLength = msg.encode(encodedBytes);

        assertArrayEquals("Encoded bytes do not match expectation", expectedBytes, encodedBytes.byteBuffer().array());
        assertEquals("Encoded length different than expected length", encodedLength, encodedBytes.position());
    }

    private byte[] generateByteArray(int bytesLength)
    {
        byte[] bytes = new byte[bytesLength];
        for(int i = 0; i < bytesLength; i++)
        {
            bytes [i] = (byte) (i % 10);
        }

        return bytes;
    }

    /*
     * http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-data
     * http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html
     *
     * ulong encodings:
     *  <encoding code="0x80" category="fixed" width="8" label="64-bit unsigned integer in network byte order"/>
     *  <encoding name="smallulong" code="0x53" category="fixed" width="1" label="unsigned long value in the range 0 to 255 inclusive"/>
     *  <encoding name="ulong0" code="0x44" category="fixed" width="0" label="the ulong value 0"/>
     *
     * binary encodings:
     *  <encoding name="vbin8" code="0xa0" category="variable" width="1" label="up to 2^8 - 1 octets of binary data"/>
     *  <encoding name="vbin32" code="0xb0" category="variable" width="4" label="up to 2^32 - 1 octets of binary data"/>
     */
    byte[] generateExpectedDataSectionBytes(final byte[] payloadBytes)
    {
        int dataBytesLength = 1;         // 0x00 for described-type constructor start
        dataBytesLength += 1;            // smallulong encoding format for data section descriptor
        dataBytesLength += 1;            // smallulong 8bit value
        dataBytesLength += 1;            // vbin variable-width binary encoding format.
        if (payloadBytes.length > 255)
        {
            dataBytesLength += 4;        // 32bit length field.
        }
        else
        {
            dataBytesLength += 1;        // 8bit length field.
        }
        dataBytesLength += payloadBytes.length; // section payload length.

        ByteBuffer buffer = ByteBuffer.allocate(dataBytesLength);

        buffer.put((byte) 0x00);                    // 0x00 for described-type constructor start
        buffer.put((byte) 0x53);                    // smallulong encoding format for data section descriptor
        buffer.put((byte) DATA_SECTION_ULONG_DESCRIPTOR); // smallulong 8bit value
        if (payloadBytes.length > 255)
        {
            buffer.put((byte) 0xb0);                // vbin32 variable-width binary encoding format.
            buffer.putInt(payloadBytes.length);     // 32bit length field.
        }
        else
        {
            buffer.put((byte) 0xa0);                // vbin8 variable-width binary encoding format.
            buffer.put((byte) payloadBytes.length); // 8bit length field.
        }
        buffer.put(payloadBytes);                   // The actual content of given length.

        assertEquals("Unexpected buffer position", dataBytesLength, buffer.position());

        return buffer.array();
    }

    private static final int READABLE_BUFFER_METHOD = 1;
    private static final int ARRAY_METHOD = 2;
    private static final int BYTE_BUFFER_METHOD = 3;

    @Test
    public void testDecodeOptionsZeroWidthArrayElementLimit() throws Exception {
        doDecodeOptionsZeroWidthArrayElementLimitTestImpl(READABLE_BUFFER_METHOD);
        doDecodeOptionsZeroWidthArrayElementLimitTestImpl(ARRAY_METHOD);
        doDecodeOptionsZeroWidthArrayElementLimitTestImpl(BYTE_BUFFER_METHOD);
    }

    private void doDecodeOptionsZeroWidthArrayElementLimitTestImpl(final int method) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put((byte) 0x00); // DescribedType
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put((byte) 0x77); // amqp-value ulong descriptor
        buffer.put((byte) 0xE0); // 'array8' type descriptor code
        buffer.put((byte) (1 + 1));
        buffer.put((byte) 1);
        buffer.put(EncodingCodes.BOOLEAN_TRUE);

        buffer.flip();

        // Decode without any options, expect to fail
        try {
            messageDecodeWithoutOptions(method, buffer.slice());
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Array element count 1 is specified to be greater than the allowed 0-width-element limit for arrays (0)", de.getMessage());
            //expected
        }

        // Decode with default options, expect to fail
        MessageDecodeOptions options = new MessageDecodeOptions();
        assertEquals(0, options.getZeroWidthArrayElementLimit());
        try {
            messageDecodeWithOptions(method, buffer.slice(), options);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Array element count 1 is specified to be greater than the allowed 0-width-element limit for arrays (0)", de.getMessage());
            //expected
        }

        // Now use the DecodeOptions to configure the setting and allow decoding it
        options.setZeroWidthArrayElementLimit(1);
        Message msg = messageDecodeWithOptions(method, buffer.slice(), options);

        assertNotNull(msg);
        assertTrue(msg.getBody() instanceof AmqpValue);
        AmqpValue body = (AmqpValue) msg.getBody();

        final boolean[] expectedValue = new boolean[]{true};

        final Object actual = body.getValue();
        assertNotNull(actual);
        assertTrue(actual.getClass().isArray());
        assertEquals(boolean.class, actual.getClass().getComponentType());

        assertArrayEquals(expectedValue, (boolean[]) actual);

        // Decode without any options again, expect to fail again
        try {
            messageDecodeWithoutOptions(method, buffer.slice());
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Array element count 1 is specified to be greater than the allowed 0-width-element limit for arrays (0)", de.getMessage());
            //expected
        }
    }

    private Message messageDecodeWithOptions(final int method, final ByteBuffer slice, final MessageDecodeOptions options) {
        Message msg = Message.Factory.create();
        switch (method) {
            case READABLE_BUFFER_METHOD:
                msg.decode(ReadableBuffer.ByteBufferReader.wrap(slice), options);
                break;
            case ARRAY_METHOD:
                msg.decode(slice.array(), slice.arrayOffset(), slice.remaining(), options);
                break;
            case BYTE_BUFFER_METHOD:
                ((MessageImpl) msg).decode(slice, options);
                break;
            default:
                throw new IllegalArgumentException("Unknown method");
        }

        return msg;
    }

    private Message messageDecodeWithoutOptions(final int method, final ByteBuffer slice) {
        Message msg = Message.Factory.create();
        switch (method) {
            case READABLE_BUFFER_METHOD:
                msg.decode(ReadableBuffer.ByteBufferReader.wrap(slice));
                break;
            case ARRAY_METHOD:
                msg.decode(slice.array(), slice.arrayOffset(), slice.remaining());
                break;
            case BYTE_BUFFER_METHOD:
                ((MessageImpl) msg).decode(slice);
                break;
            default:
                throw new IllegalArgumentException("Unknown method");
        }

        return msg;
    }

    @Test
    public void testDecodeOptionsDepthWithMessageWithNestedListOfLists() throws Exception {
        doDecodeOptionsDepthWithMessageWithNestedListOfListsTestImpl(READABLE_BUFFER_METHOD);
        doDecodeOptionsDepthWithMessageWithNestedListOfListsTestImpl(ARRAY_METHOD);
        doDecodeOptionsDepthWithMessageWithNestedListOfListsTestImpl(BYTE_BUFFER_METHOD);
    }

    private void doDecodeOptionsDepthWithMessageWithNestedListOfListsTestImpl(final int method) {
        DecoderImpl decoder = new DecoderImpl();
        EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        ByteBuffer buffer = ByteBuffer.allocate(1024);

        final List<Object> toEncode = new ArrayList<>();

        // Encodes more than the default max depth value allowed
        final int depthLimit = 32 + 1;
        List<Object> current = toEncode;
        for (int i = 1; i < depthLimit; ++i) {
            final List<Object> next = new ArrayList<>();

            current.add(next);
            current = next;
        }

        encoder.setByteBuffer(buffer);
        encoder.writeObject(new AmqpValue(toEncode));

        buffer.flip();

        // Decode without any options, expect to fail
        try {
            messageDecodeWithoutOptions(method, buffer.slice());
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 32", de.getMessage());
            //expected
        }

        // Decode with default options, expect to fail
        MessageDecodeOptions options = new MessageDecodeOptions();
        assertEquals(32, options.getMaxDecodeDepth());
        try {
            messageDecodeWithOptions(method, buffer.slice(), options);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 32", de.getMessage());
            //expected
        }

        // Now use the DecodeOptions to configure the setting and allow decoding it
        options.setMaxDecodeDepth(depthLimit);
        Message msg = messageDecodeWithOptions(method, buffer.slice(), options);

        assertNotNull(msg);
        assertTrue(msg.getBody() instanceof AmqpValue);
        AmqpValue body = (AmqpValue) msg.getBody();

        final Object actual = body.getValue();
        assertNotNull(actual instanceof List);

        assertEquals(toEncode, actual);

        // Decode without any options again, expect to fail again
        try {
            messageDecodeWithoutOptions(method, buffer.slice());
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 32", de.getMessage());
            //expected
        }
    }
}
