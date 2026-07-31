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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for the Proton Binary type encoder / decoder
 */
public class BinaryTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testEncodeSmallBinaryReservesSpaceForPayload() throws IOException {
        doTestEncodeBinaryTypeReservation(SMALL_SIZE);
    }

    @Test
    public void testEncodeLargeBinaryReservesSpaceForPayload() throws IOException {
        doTestEncodeBinaryTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeBinaryTypeReservation(int size) throws IOException {
        byte[] data = new byte[size];
        for (int i = 0; i < size; ++i) {
            data[i] = (byte) (i % 255);
        }

        Binary binary = new Binary(data);

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeBinary(binary);

        // Check that the BinaryType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
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
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeBinaryWithSpecificSizeTestImpl(false, 5 + 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("binary length 6 cannot be more than remaining bytes 5", de.getMessage());
        }

        try {
            doDecodeBinaryWithSpecificSizeTestImpl(true, 5 + 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("binary length 6 cannot be more than remaining bytes 5", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeBinaryWithSpecificSizeTestImpl(true, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("binary length -1 cannot be negative", de.getMessage());
        }
    }

    private Object doDecodeBinaryWithSpecificSizeTestImpl(final boolean sym32, int size) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        if (sym32) {
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
