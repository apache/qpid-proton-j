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

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface Decoder
{
    public static interface ListProcessor<T>
    {
        T process(int count, Encoder encoder);
    }


    Boolean readBoolean();
    Boolean readBoolean(Boolean defaultVal);
    boolean readBoolean(boolean defaultVal);

    Byte readByte();
    Byte readByte(Byte defaultVal);
    byte readByte(byte defaultVal);

    Short readShort();
    Short readShort(Short defaultVal);
    short readShort(short defaultVal);

    Integer readInteger();
    Integer readInteger(Integer defaultVal);
    int readInteger(int defaultVal);

    Long readLong();
    Long readLong(Long defaultVal);
    long readLong(long defaultVal);

    UnsignedByte readUnsignedByte();
    UnsignedByte readUnsignedByte(UnsignedByte defaultVal);

    UnsignedShort readUnsignedShort();
    UnsignedShort readUnsignedShort(UnsignedShort defaultVal);

    UnsignedInteger readUnsignedInteger();
    UnsignedInteger readUnsignedInteger(UnsignedInteger defaultVal);

    UnsignedLong readUnsignedLong();
    UnsignedLong readUnsignedLong(UnsignedLong defaultVal);

    Character readCharacter();
    Character readCharacter(Character defaultVal);
    char readCharacter(char defaultVal);

    Float readFloat();
    Float readFloat(Float defaultVal);
    float readFloat(float defaultVal);

    Double readDouble();
    Double readDouble(Double defaultVal);
    double readDouble(double defaultVal);

    UUID readUUID();
    UUID readUUID(UUID defaultValue);

    Decimal32 readDecimal32();
    Decimal32 readDecimal32(Decimal32 defaultValue);

    Decimal64 readDecimal64();
    Decimal64 readDecimal64(Decimal64 defaultValue);

    Decimal128 readDecimal128();
    Decimal128 readDecimal128(Decimal128 defaultValue);

    Date readTimestamp();
    Date readTimestamp(Date defaultValue);

    Binary readBinary();
    Binary readBinary(Binary defaultValue);

    Symbol readSymbol();
    Symbol readSymbol(Symbol defaultValue);

    String readString();
    String readString(String defaultValue);

    List readList();
    <T> void readList(ListProcessor<T> processor);

    Map readMap();

    <T> T[] readArray(Class<T> clazz);

    Object[] readArray();

    boolean[] readBooleanArray();
    byte[] readByteArray();
    short[] readShortArray();
    int[] readIntegerArray();
    long[] readLongArray();
    float[] readFloatArray();
    double[] readDoubleArray();
    char[] readCharacterArray();

    <T> T[] readMultiple(Class<T> clazz);

    Object[] readMultiple();
    byte[] readByteMultiple();
    short[] readShortMultiple();
    int[] readIntegerMultiple();
    long[] readLongMultiple();
    float[] readFloatMultiple();
    double[] readDoubleMultiple();
    char[] readCharacterMultiple();

    Object readObject();
    Object readObject(Object defaultValue);

    /**
     * Returns the maximum number of array elements allowed while decoding arrays of zero-width
     * elements (ulong0, uint0, list0, boolean-true, boolean-false, null) before a decode exception.
     *
     * @return the maximum number of zero-width elements allowed in an array.
     */
    int getZeroWidthArrayElementLimit();

    /**
     * Configures the maximum number of array elements allowed while decoding arrays of zero-width
     * elements (ulong0, uint0, list0, boolean-true, boolean-false, null) before a decode exception.
     * Default is 0, meaning only the empty array is permitted.
     */
    void setZeroWidthArrayElementLimit(int zeroWidthElementLimit);

    /**
     * Returns the maximum decode depth allowed before error when decoding Maps, Lists,
     * Arrays, and certain related DescribedTypes from an encoded payload.
     *
     * @return the maximum depth limit before error for Maps, Lists, Arrays and related DescribedTypes.
     */
    int getMaxDecodeDepth();

    /**
     * Configures the maximum decode depth allowed before error when decoding Maps, Lists,
     * Arrays, and certain related DescribedTypes from an encoded payload. Default is 50.
     */
    void setMaxDecodeDepth(int maxDecodeDepth);

    void register(final Object descriptor, final DescribedTypeConstructor dtc);

    void register(final Object descriptor, final FastPathDescribedTypeConstructor<?> dtc);

}
