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
import static org.junit.Assert.fail;

import org.junit.Test;

public class DecoderImplTest extends CodecTestSupport {

    @Test
    public void testReadBytePrimitive() {
        buffer.put(EncodingCodes.BYTE);
        buffer.put((byte) 7);
        buffer.put(EncodingCodes.BYTE);
        buffer.put(Byte.MAX_VALUE);
        buffer.put(EncodingCodes.BYTE);
        buffer.put(Byte.MIN_VALUE);
        buffer.put(EncodingCodes.BYTE);
        buffer.put((byte) 0);
        buffer.put(EncodingCodes.NULL);
        buffer.put(EncodingCodes.SHORT);

        buffer.flip();

        final byte defaultVal = (byte) 3;
        assertEquals(7, decoder.readByte(defaultVal));
        assertEquals(Byte.MAX_VALUE, decoder.readByte(defaultVal));
        assertEquals(Byte.MIN_VALUE, decoder.readByte(defaultVal));
        assertEquals(0, decoder.readByte(defaultVal));

        // Returns the default
        assertEquals(defaultVal, decoder.readByte(defaultVal));

        try {
            decoder.readByte(defaultVal);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Expected Byte type but found encoding: SHORT:0x61", de.getMessage());
        }
    }

    @Test
    public void testReadShortPrimitive() {
        buffer.put(EncodingCodes.SHORT);
        buffer.putShort((short) 7);
        buffer.put(EncodingCodes.SHORT);
        buffer.putShort(Short.MAX_VALUE);
        buffer.put(EncodingCodes.SHORT);
        buffer.putShort(Short.MIN_VALUE);
        buffer.put(EncodingCodes.SHORT);
        buffer.putShort((short) 0);
        buffer.put(EncodingCodes.NULL);
        buffer.put(EncodingCodes.INT);

        buffer.flip();

        final short defaultVal = (short) 3;
        assertEquals(7, decoder.readShort(defaultVal));
        assertEquals(Short.MAX_VALUE, decoder.readShort(defaultVal));
        assertEquals(Short.MIN_VALUE, decoder.readShort(defaultVal));
        assertEquals(0, decoder.readShort(defaultVal));

        // Returns the default
        assertEquals(defaultVal, decoder.readShort(defaultVal));

        try {
            decoder.readShort(defaultVal);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Expected Short type but found encoding: INT:0x71", de.getMessage());
        }
    }

    @Test
    public void testReadIntPrimitive() {
        buffer.put(EncodingCodes.INT);
        buffer.putInt(7);
        buffer.put(EncodingCodes.INT);
        buffer.putInt(Integer.MAX_VALUE);
        buffer.put(EncodingCodes.INT);
        buffer.putInt(Integer.MIN_VALUE);
        buffer.put(EncodingCodes.INT);
        buffer.putInt(0);
        buffer.put(EncodingCodes.NULL);
        buffer.put(EncodingCodes.LONG);

        buffer.flip();

        final int defaultVal = 3;
        assertEquals(7, decoder.readInteger(defaultVal));
        assertEquals(Integer.MAX_VALUE, decoder.readInteger(defaultVal));
        assertEquals(Integer.MIN_VALUE, decoder.readInteger(defaultVal));
        assertEquals(0, decoder.readInteger(defaultVal));

        // Returns the default
        assertEquals(defaultVal, decoder.readInteger(defaultVal));

        try {
            decoder.readInteger(defaultVal);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Expected Integer type but found encoding: LONG:0x81", de.getMessage());
        }
    }

    @Test
    public void testReadLongPrimitive() {
        buffer.put(EncodingCodes.LONG);
        buffer.putLong(7);
        buffer.put(EncodingCodes.LONG);
        buffer.putLong(Long.MAX_VALUE);
        buffer.put(EncodingCodes.LONG);
        buffer.putLong(Long.MIN_VALUE);
        buffer.put(EncodingCodes.LONG);
        buffer.putLong(0);
        buffer.put(EncodingCodes.NULL);
        buffer.put(EncodingCodes.INT);

        buffer.flip();

        final long defaultVal = 3;
        assertEquals(7, decoder.readLong(defaultVal));
        assertEquals(Long.MAX_VALUE, decoder.readLong(defaultVal));
        assertEquals(Long.MIN_VALUE, decoder.readLong(defaultVal));
        assertEquals(0, decoder.readLong(defaultVal));

        // Returns the default
        assertEquals(defaultVal, decoder.readLong(defaultVal));

        try {
            decoder.readLong(defaultVal);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Expected Long type but found encoding: INT:0x71", de.getMessage());
        }
    }

    @Test
    public void testReadFloatPrimitive() {
        buffer.put(EncodingCodes.FLOAT);
        buffer.putFloat((float)7.0);
        buffer.put(EncodingCodes.FLOAT);
        buffer.putFloat(Float.MAX_VALUE);
        buffer.put(EncodingCodes.FLOAT);
        buffer.putFloat(Float.MIN_VALUE);
        buffer.put(EncodingCodes.FLOAT);
        buffer.putFloat((float) 0.0);
        buffer.put(EncodingCodes.NULL);
        buffer.put(EncodingCodes.INT);

        buffer.flip();

        final float defaultVal = (float) 3.1;
        assertEquals((float) 7.0, decoder.readFloat(defaultVal), 0.00);
        assertEquals(Float.MAX_VALUE, decoder.readFloat(defaultVal), 0.00);
        assertEquals(Float.MIN_VALUE, decoder.readFloat(defaultVal), 0.00);
        assertEquals((float) 0.0, decoder.readFloat(defaultVal), 0.00);

        // Returns the default
        assertEquals(defaultVal, decoder.readFloat(defaultVal),0.00);

        try {
            decoder.readFloat(defaultVal);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Expected Float type but found encoding: INT:0x71", de.getMessage());
        }
    }

    @Test
    public void testReadDoublePrimitive() {
        buffer.put(EncodingCodes.DOUBLE);
        buffer.putDouble((float)7.0);
        buffer.put(EncodingCodes.DOUBLE);
        buffer.putDouble(Double.MAX_VALUE);
        buffer.put(EncodingCodes.DOUBLE);
        buffer.putDouble(Double.MIN_VALUE);
        buffer.put(EncodingCodes.DOUBLE);
        buffer.putDouble((float) 0.0);
        buffer.put(EncodingCodes.NULL);
        buffer.put(EncodingCodes.INT);

        buffer.flip();

        final double defaultVal = (double) 3.1;
        assertEquals((double) 7.0, decoder.readDouble(defaultVal), 0.00);
        assertEquals(Double.MAX_VALUE, decoder.readDouble(defaultVal), 0.00);
        assertEquals(Double.MIN_VALUE, decoder.readDouble(defaultVal), 0.00);
        assertEquals((double) 0.0, decoder.readDouble(defaultVal), 0.00);

        // Returns the default
        assertEquals(defaultVal, decoder.readDouble(defaultVal),0.00);

        try {
            decoder.readDouble(defaultVal);
            fail("Expected exception");
        } catch (DecodeException de) {
            assertEquals("Expected Double type but found encoding: INT:0x71", de.getMessage());
        }
    }
}
