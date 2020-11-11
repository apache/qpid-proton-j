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
package org.apache.qpid.proton.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.Character.UnicodeBlock;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the encoding and decoding of {@link StringType} values.
 */
public class StringTypeTest
{
    private static final List<String> TEST_DATA = generateTestData();

    /**
     * Loop over all the chars in given {@link UnicodeBlock}s and return a {@link Set <String>}
     * containing all the possible values as their {@link String} values.
     *
     * @param blocks
     *        the {@link UnicodeBlock}s to loop over
     * @return a {@link Set <String>} containing all the possible values as {@link String}
     *         values
     */
    private static Set<String> getAllStringsFromUnicodeBlocks(final UnicodeBlock... blocks)
    {
        final Set<UnicodeBlock> blockSet = new HashSet<>(Arrays.asList(blocks));
        final Set<String> strings = new HashSet<>();
        for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++)
        {
            if (blockSet.contains(UnicodeBlock.of(codePoint)))
            {
                final int charCount = Character.charCount(codePoint);
                final StringBuilder sb = new StringBuilder(
                        charCount);
                if (charCount == 1)
                {
                    sb.append(String.valueOf((char) codePoint));
                }
                else if (charCount == 2)
                {
                    sb.append(Character.highSurrogate(codePoint));
                    sb.append(Character.lowSurrogate(codePoint));
                }
                else
                {
                    throw new IllegalArgumentException("Character.charCount of "
                                                       + charCount + " not supported.");
                }
                strings.add(sb.toString());
            }
        }
        return strings;
    }

    /**
     * Test the encoding and decoding of various complicated Unicode characters which will end
     * up as "surrogate pairs" when encoded to UTF-8
     */
    @Test
    public void calculateUTF8Length()
    {
        for (final String input : TEST_DATA)
        {
            assertEquals("Incorrect string length calculated for string '"+input+"'",input.getBytes(StandardCharsets.UTF_8).length, StringType.calculateUTF8Length(input));
        }
    }

    /**
     * Test the encoding and decoding of various Unicode characters
     */
    @Test
    public void encodeDecodeStrings()
    {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        final ByteBuffer bb = ByteBuffer.allocate(16);

        for (final String input : TEST_DATA)
        {
            bb.clear();
            final AmqpValue inputValue = new AmqpValue(input);
            encoder.setByteBuffer(bb);
            encoder.writeObject(inputValue);
            bb.clear();
            decoder.setByteBuffer(bb);
            final AmqpValue outputValue = (AmqpValue) decoder.readObject();
            assertEquals("Failed to round trip String correctly: ", input, outputValue.getValue());
        }
    }

    @Test
    public void testSkipString()
    {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        final ByteBuffer buffer = ByteBuffer.allocate(64);

        decoder.setByteBuffer(buffer);
        encoder.setByteBuffer(buffer);

        encoder.writeString("skipped");
        encoder.writeString("read");

        buffer.clear();

        TypeConstructor<?> stringType = decoder.readConstructor();
        assertEquals(String.class, stringType.getTypeClass());
        stringType.skipValue();

        String result = decoder.readString();
        assertEquals("read", result);
    }

    @Test
    public void testEncodeAndDecodeEmptyString() {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        final ByteBuffer buffer = ByteBuffer.allocate(64);

        encoder.setByteBuffer(buffer);
        decoder.setByteBuffer(buffer);

        encoder.writeString("a");
        encoder.writeString("");
        encoder.writeString("b");

        buffer.clear();

        TypeConstructor<?> stringType = decoder.readConstructor();
        assertEquals(String.class, stringType.getTypeClass());
        stringType.skipValue();

        String result = decoder.readString();
        assertEquals("", result);
        result = decoder.readString();
        assertEquals("b", result);
    }

    @Test
    public void testEmptyShortStringEncode() {
        doTestEmptyStringEncodeAsGivenType(EncodingCodes.STR8);
    }

    @Test
    public void testEmptyLargeStringEncode() {
        doTestEmptyStringEncodeAsGivenType(EncodingCodes.STR32);
    }

    public void doTestEmptyStringEncodeAsGivenType(byte encodingCode) {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        final ByteBuffer buffer = ByteBuffer.allocate(64);

        buffer.put(encodingCode);
        buffer.putInt(0);
        buffer.clear();

        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);

        CompositeReadableBuffer composite = new CompositeReadableBuffer();
        composite.append(copy);

        decoder.setBuffer(composite);

        TypeConstructor<?> stringType = decoder.readConstructor();
        assertEquals(String.class, stringType.getTypeClass());

        String result = (String) stringType.readValue();
        assertEquals("", result);
    }

    @Test
    public void testDecodeNonStringWhenStringExpectedReportsUsefulError() {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);

        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        final ByteBuffer buffer = ByteBuffer.allocate(64);
        final UUID encoded = UUID.randomUUID();

        buffer.put(EncodingCodes.UUID);
        buffer.putLong(encoded.getMostSignificantBits());
        buffer.putLong(encoded.getLeastSignificantBits());
        buffer.flip();

        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);

        CompositeReadableBuffer composite = new CompositeReadableBuffer();
        composite.append(copy);

        decoder.setBuffer(composite);

        TypeConstructor<?> stringType = decoder.peekConstructor();
        assertEquals(UUID.class, stringType.getTypeClass());

        composite.mark();

        try {
            decoder.readString();
        } catch (DecodeException ex) {
            // Should indicate the type that it found in the error
            assertTrue(ex.getMessage().contains(EncodingCodes.toString(EncodingCodes.UUID)));
        }

        composite.reset();
        UUID actual = decoder.readUUID();
        assertEquals(encoded, actual);
    }

    @Test
    public void testDecodeUnknownTypeWhenStringExpectedReportsUsefulError() {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);

        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        final ByteBuffer buffer = ByteBuffer.allocate(64);

        buffer.put((byte) 0x01);
        buffer.flip();

        byte[] copy = new byte[buffer.remaining()];
        buffer.get(copy);

        CompositeReadableBuffer composite = new CompositeReadableBuffer();
        composite.append(copy);

        decoder.setBuffer(composite);

        try {
            decoder.readString();
        } catch (DecodeException ex) {
            // Should indicate the type that it found in the error
            assertTrue(ex.getMessage().contains("Unknown-Type:0x01"));
        }
    }

    // build up some test data with a set of suitable Unicode characters
    private static List<String> generateTestData()
    {
        return new LinkedList<String>()
        {
            private static final long serialVersionUID = 7331717267070233454L;
            {
                // non-surrogate pair blocks
                addAll(getAllStringsFromUnicodeBlocks(UnicodeBlock.BASIC_LATIN,
                                                     UnicodeBlock.LATIN_1_SUPPLEMENT,
                                                     UnicodeBlock.GREEK,
                                                     UnicodeBlock.LETTERLIKE_SYMBOLS));
                // blocks with surrogate pairs
                addAll(getAllStringsFromUnicodeBlocks(UnicodeBlock.LINEAR_B_SYLLABARY,
                                                     UnicodeBlock.MISCELLANEOUS_SYMBOLS_AND_PICTOGRAPHS,
                                                     UnicodeBlock.MUSICAL_SYMBOLS,
                                                     UnicodeBlock.EMOTICONS,
                                                     UnicodeBlock.PLAYING_CARDS,
                                                     UnicodeBlock.BOX_DRAWING,
                                                     UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS,
                                                     UnicodeBlock.PRIVATE_USE_AREA,
                                                     UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_A,
                                                     UnicodeBlock.SUPPLEMENTARY_PRIVATE_USE_AREA_B));
                // some additional combinations of characters that could cause problems to the encoder
                String[] boxDrawing = getAllStringsFromUnicodeBlocks(UnicodeBlock.BOX_DRAWING).toArray(new String[0]);
                String[] halfFullWidthForms = getAllStringsFromUnicodeBlocks(UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS).toArray(new String[0]);
                for (int i = 0; i < halfFullWidthForms.length; i++)
                {
                    add(halfFullWidthForms[i] + boxDrawing[i % boxDrawing.length]);
                }
            }
        };
    }

    @Test
    public void testEncodeSmallStringReservesSpaceForPayload() throws IOException {
        doTestEncodeStringTypeReservation(32);
    }

    @Test
    public void testEncodeLargeStringReservesSpaceForPayload() throws IOException {
        doTestEncodeStringTypeReservation(512);
    }

    private void doTestEncodeStringTypeReservation(int size) throws IOException {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        StringBuilder builder = new StringBuilder(size);
        for (int i = 0; i < size; ++i) {
            builder.append(i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(ByteBuffer.allocate(2048));
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeString(builder.toString());

        // Check that the StringType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testEncodeAndDecodeUsingWritableBufferDefaultPutString() throws Exception {
        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);

        // Verify that the default put(String) impl is being used by the buffers
        Method m = WritableBufferWithoutPutStringOverride.class.getMethod("put", String.class);
        assertTrue("Expected method to be default", m.isDefault());

        for (final String input : TEST_DATA) {
            final WritableBufferWithoutPutStringOverride sink = new WritableBufferWithoutPutStringOverride(16);
            final AmqpValue inputValue = new AmqpValue(input);
            encoder.setByteBuffer(sink);
            encoder.writeObject(inputValue);
            ReadableBuffer source = new ReadableBuffer.ByteBufferReader(ByteBuffer.wrap(sink.getArray(), 0, sink.getArrayLength()));
            decoder.setBuffer(source);
            final AmqpValue outputValue = (AmqpValue) decoder.readObject();
            assertEquals("Failed to round trip String correctly: ", input, outputValue.getValue());
        }
    }

    /**
     * Test class which implements WritableBuffer but does not override the default put(String)
     * method, used to verify the default method is in place and works.
     */
    private static final class WritableBufferWithoutPutStringOverride implements WritableBuffer {

        private final ByteBufferWrapper delegate;

        public WritableBufferWithoutPutStringOverride(int capacity) {
            delegate = WritableBuffer.ByteBufferWrapper.allocate(capacity);
        }

        public byte[] getArray() {
            return delegate.byteBuffer().array();
        }

        public int getArrayLength() {
            return delegate.byteBuffer().position();
        }

        @Override
        public void put(byte b) {
            delegate.put(b);
        }

        @Override
        public void putShort(short value) {
            delegate.putShort(value);
        }

        @Override
        public void putInt(int value) {
            delegate.putInt(value);
        }

        @Override
        public void putLong(long value) {
            delegate.putLong(value);
        }

        @Override
        public void putFloat(float value) {
            delegate.putFloat(value);
        }

        @Override
        public void putDouble(double value) {
            delegate.putDouble(value);
        }

        @Override
        public void put(byte[] src, int offset, int length) {
            delegate.put(src, offset, length);
        }

        @Override
        public boolean hasRemaining() {
            return delegate.hasRemaining();
        }

        @Override
        public int remaining() {
            return delegate.remaining();
        }

        @Override
        public int position() {
            return delegate.position();
        }

        @Override
        public void position(int position) {
            delegate.position(position);
        }

        @Override
        public void put(ByteBuffer payload) {
            delegate.put(payload);
        }

        @Override
        public int limit() {
            return delegate.limit();
        }

        @Override
        public void put(ReadableBuffer src) {
            delegate.put(src);
        }
    }

    @Test
    public void testEncodeAndDecodeLargeUnicodeString() throws IOException {
        StringBuilder unicodeStringBuilder = new StringBuilder();

        unicodeStringBuilder.append((char) 1000);
        unicodeStringBuilder.append((char) 1001);
        unicodeStringBuilder.append((char) 1002);
        unicodeStringBuilder.append((char) 1003);

        final DecoderImpl decoder = new DecoderImpl();
        final EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
        final ByteBuffer bb = ByteBuffer.allocate(1024);

        final AmqpValue inputValue = new AmqpValue(unicodeStringBuilder.toString());
        encoder.setByteBuffer(bb);
        encoder.writeObject(inputValue);

        final int size1 = bb.position() / 2;
        final int size2 = bb.position() - size1;

        final byte[] slice1 = new byte[size1];
        final byte[] slice2 = new byte[size2];

        bb.flip();
        bb.get(slice1);
        bb.get(slice2);

        CompositeReadableBuffer composite = new CompositeReadableBuffer();
        composite.append(slice1);
        composite.append(slice2);

        decoder.setBuffer(composite);

        final AmqpValue outputValue = (AmqpValue) decoder.readObject();

        assertEquals("Failed to round trip String correctly: ", unicodeStringBuilder.toString(), outputValue.getValue());
    }
}
