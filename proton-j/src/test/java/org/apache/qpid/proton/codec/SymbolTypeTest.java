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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.function.Function;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the encoding and decoding of {@link SymbolType} values.
 */
public class SymbolTypeTest extends CodecTestSupport {

    private final int LARGE_SIZE = 512;
    private final int SMALL_SIZE = 32;

    @Test
    public void testEncodeSmallSymbolReservesSpaceForPayload() throws IOException {
        doTestEncodeSymbolTypeReservation(SMALL_SIZE);
    }

    @Test
    public void testEncodeLargeSymbolReservesSpaceForPayload() throws IOException {
        doTestEncodeSymbolTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeSymbolTypeReservation(int size) throws IOException {
        Random random = new Random(System.currentTimeMillis());
        StringBuilder builder = new StringBuilder(size);
        for (int i = 0; i < size; ++i) {
            builder.append((byte) random.nextInt(127));
        }

        Symbol symbol = Symbol.valueOf(builder.toString());

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(ByteBuffer.allocate(2048));
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeSymbol(symbol);

        // Check that the SymbolType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testDecodeEncodeSymbol64() {
        doDecodeEncodeSymbolTestImpl(decoder, encoder, "testDecodeEncodeSymbol64-", 64, true, Symbol::getSymbol);
    }

    @Test
    public void testDecodeEncodeSymbol65() {
        doDecodeEncodeSymbolTestImpl(decoder, encoder, "testDecodeEncodeSymbol65-", 65, false, Symbol::getSymbol);
    }

    @Test
    public void testDecodeEncodeSymbol384() {
        doDecodeEncodeSymbolTestImpl(decoder, encoder, "testDecodeEncodeSymbol384-", 384, false, Symbol::getSymbol);
    }

    @Test
    public void testDecodeEncodeSaslSymbol32() {
        final DecoderImpl saslDecoder = new DecoderImpl();
        final EncoderImpl saslEncoder = new EncoderImpl(saslDecoder, true);
        AMQPDefinedTypes.registerSecurityTypes(saslDecoder, saslEncoder);

        final Symbol decoded = doDecodeEncodeSymbolTestImpl(saslDecoder, saslEncoder, "testSaslSymbol32-", 32, true, Symbol::getSaslSymbol);
        assertEquals(32, decoded.length());

        final Symbol lookupSasl = Symbol.getSaslSymbol(decoded.toString()); // Sasl first to try its cache before main is populated and found
        final Symbol lookupMain = Symbol.getSymbol(decoded.toString());

        assertEquals(decoded, lookupSasl);
        assertSame(decoded, lookupSasl);

        assertEquals(decoded, lookupMain);
        assertNotSame(decoded, lookupMain);

        assertNotSame(lookupSasl, lookupMain);
    }

    @Test
    public void testDecodeEncodeSaslSymbol33() {
        final DecoderImpl saslDecoder = new DecoderImpl();
        final EncoderImpl saslEncoder = new EncoderImpl(saslDecoder, true);
        AMQPDefinedTypes.registerSecurityTypes(saslDecoder, saslEncoder);

        final Symbol decoded = doDecodeEncodeSymbolTestImpl(saslDecoder, saslEncoder, "testSaslSymbol33-", 33, false, Symbol::getSaslSymbol);
        assertEquals(33, decoded.length());

        final Symbol lookupSasl = Symbol.getSaslSymbol(decoded.toString()); // Sasl first to try its cache before main is populated and found
        final Symbol lookupMain = Symbol.getSymbol(decoded.toString());

        assertEquals(decoded, lookupSasl);
        assertNotSame(decoded, lookupSasl); // Exceeds Sasl cache limit

        assertEquals(decoded, lookupMain);
        assertNotSame(decoded, lookupMain);

        assertNotSame(lookupSasl, lookupMain);
    }

    @Test
    public void testDecodeEncodeSaslSymbol384() {
        final DecoderImpl saslDecoder = new DecoderImpl();
        final EncoderImpl saslEncoder = new EncoderImpl(saslDecoder, true);
        AMQPDefinedTypes.registerSecurityTypes(saslDecoder, saslEncoder);

        final Symbol decoded = doDecodeEncodeSymbolTestImpl(saslDecoder, saslEncoder, "testSaslSymbol384-", 384, false, Symbol::getSaslSymbol);
        assertEquals(384, decoded.length());

        final Symbol lookupSasl = Symbol.getSaslSymbol(decoded.toString());
        final Symbol lookupMain = Symbol.getSymbol(decoded.toString());

        assertEquals(decoded, lookupSasl);
        assertNotSame(decoded, lookupSasl); // Exceeds Sasl cache limit

        assertEquals(decoded, lookupMain);
        assertNotSame(decoded, lookupMain);

        assertNotSame(lookupSasl, lookupMain);
    }

    @Test
    @Ignore("Cache is static so running has side effects.")
    public void testDecodeSymbolCacheMaxesOut() {
        doDecodeSymbolCacheMaxesOutTestImpl(false);
    }

    @Test
    @Ignore("Cache is static so running has side effects.")
    public void testDecodeSymbolCacheMaxesOutSym32() {
        doDecodeSymbolCacheMaxesOutTestImpl(true);
    }

    private void doDecodeSymbolCacheMaxesOutTestImpl(boolean sym32) {
        boolean notSame = false;
        int attempts = 0;
        final String prefix = "testDecodeSymbolCacheMaxesOut-";

        for (attempts = 0; attempts <= Short.MAX_VALUE; attempts++) {
            final String namePrefix = prefix + String.valueOf(attempts);

            final byte[] valueBytes = createPayloadSymbolBytes(namePrefix, 64);

            final Symbol symbol1 = doDecodeSymbolTestImpl(decoder, valueBytes, 64, sym32);
            final Symbol symbol2 = doDecodeSymbolTestImpl(decoder, valueBytes, 64, sym32);

            if (symbol1 != symbol2) {
                notSame = true;
                break;
            }
        }

        assertTrue(notSame);
        assertTrue(attempts > 500);
        assertTrue(attempts <= 4096);
    }

    private Symbol doDecodeSymbolTestImpl(final DecoderImpl decoderImpl, final byte[] valueBytes, final int symbolLength, boolean sym32) {
        assertEquals("Unexpected bytes length", symbolLength, valueBytes.length);

        if (symbolLength > 255 && !sym32) {
            throw new IllegalArgumentException();
        }

        final int encodedSize = 1 + (sym32 ? 4 : 1) + symbolLength; // type code, size, and value bytes

        ByteBuffer decodingBuf = ByteBuffer.allocate(encodedSize);

        decodingBuf.put(sym32 ? (byte) 0xB3 : (byte) 0xA3); // The 'sym32' or 'sym8' type descriptor code
        if (sym32) {
            decodingBuf.putInt(symbolLength); // The 4 byte length of the symbol bytes
        } else {
            decodingBuf.put((byte) symbolLength); // The 1 byte length of the symbol bytes
        }
        decodingBuf.put(valueBytes); // The bytes of the symbol itself

        assertEquals("Should have filled encoding array", 0, decodingBuf.remaining());

        // Decode
        decodingBuf.rewind();
        decoderImpl.setByteBuffer(decodingBuf);
        final Object decoded = decoderImpl.readObject();

        assertEquals("Should have read all byes from encoding array", 0, decodingBuf.remaining());

        assertNotNull(decoded);
        assertEquals(Symbol.class, decoded.getClass());

        return (Symbol) decoded;
    }

    private Symbol doDecodeEncodeSymbolTestImpl(final DecoderImpl decoderImpl, final EncoderImpl encoderImpl, final String prefix,
                                      final int symbolLength, final boolean expectSame, final Function<String, Symbol> supplier) {
        final byte[] valueBytes = createPayloadSymbolBytes(prefix, symbolLength);

        assertEquals("Unexpected bytes length", symbolLength, valueBytes.length);

        final int encodedSize = 1 + (symbolLength <= 255 ? 1 : 4) + symbolLength; // type code, size, and value bytes

        ByteBuffer decodingBuf = ByteBuffer.allocate(encodedSize);

        decodingBuf.put(symbolLength <= 255 ? (byte) 0xA3 : (byte) 0xB3); // The 'sym8' or 'sym32' type descriptor code
        if (symbolLength <= 255) {
            decodingBuf.put((byte) symbolLength); // The 1 byte length of the symbol bytes
        } else {
            decodingBuf.putInt(symbolLength); // The 4 byte length of the symbol bytes
        }
        decodingBuf.put(valueBytes); // The bytes of the symbol itself

        assertEquals("Should have filled encoding array", 0, decodingBuf.remaining());

        // Decode
        decodingBuf.rewind();
        decoderImpl.setByteBuffer(decodingBuf);
        final Object decoded = decoderImpl.readObject();

        assertNotNull(decoded);
        assertEquals(Symbol.class, decoded.getClass());

        assertEquals("Should have read all byes from encoding array", 0, decodingBuf.remaining());

        // Decode again and compare
        decodingBuf.rewind();
        decoderImpl.setByteBuffer(decodingBuf);
        final Object decoded2 = decoderImpl.readObject();

        assertNotNull(decoded2);
        assertEquals(Symbol.class, decoded2.getClass());

        assertEquals(decoded, decoded2);

        if (expectSame) {
            assertSame(decoded, decoded2);
        } else {
            assertNotSame(decoded, decoded2);
        }

        // Encode the symbol and compare to decoding bytes
        ByteBuffer encodingBuf = ByteBuffer.allocate(encodedSize);
        encoderImpl.setByteBuffer(encodingBuf);

        encoderImpl.writeObject(decoded);

        assertEquals("Should have filled encoding array", 0, encodingBuf.remaining());

        assertArrayEquals(decodingBuf.array(), encodingBuf.array());

        // Look up the value without decoder or encoder involved, expect same/different objects as before
        final String stringValue = new String(valueBytes, StandardCharsets.US_ASCII);
        final Symbol lookup = supplier.apply(stringValue);
        if (expectSame) {
            assertSame(decoded, lookup);
        } else {
            assertNotSame(decoded, lookup);
            assertNotSame(decoded2, lookup);
        }

        assertEquals(lookup, decoded);

        return (Symbol) decoded;
    }

    // Creates bytes for symbols
    public static byte[] createPayloadSymbolBytes(String prefix, int length) {
        byte[] testNameBytes = prefix.getBytes(StandardCharsets.US_ASCII);
        assertTrue(testNameBytes.length < length);

        byte[] payload = new byte[length];

        System.arraycopy(testNameBytes, 0, payload, 0, testNameBytes.length);

        Random rand = new Random(System.currentTimeMillis());

        for (int i = testNameBytes.length; i < length; i++) {
            payload[i] = (byte) rand.nextInt(10);
        }

        return payload;
    }

    @Test
    public void testDecodeSymbol() throws Exception {
        Object o = doDecodeSymbolWithSpecificSizeTestImpl(false, 5);
        assertEquals(Symbol.valueOf("54321"), o);

        o = doDecodeSymbolWithSpecificSizeTestImpl(true, 5);
        assertEquals(Symbol.valueOf("54321"), o);
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeSymbolWithSpecificSizeTestImpl(false, 5 + 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("symbol length 6 cannot be more than remaining bytes 5", de.getMessage());
        }

        try {
            doDecodeSymbolWithSpecificSizeTestImpl(true, 5 + 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("symbol length 6 cannot be more than remaining bytes 5", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeSymbolWithSpecificSizeTestImpl(true, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("symbol length -1 cannot be negative", de.getMessage());
        }
    }

    private Object doDecodeSymbolWithSpecificSizeTestImpl(final boolean sym32, int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
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
        int oldLimit = tmpBuf.limit();

        decoder.setByteBuffer(tmpBuf);

        final Object decoded;
        try {
            decoded = decoder.readObject();
        } finally {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }
}
