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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Decimal128;
import org.apache.qpid.proton.amqp.Decimal32;
import org.apache.qpid.proton.amqp.Decimal64;
import org.apache.qpid.proton.amqp.DescribedType;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;

public class DecoderImpl implements ByteBufferDecoder
{
    private ReadableBuffer _buffer;

    private final CharsetDecoder _charsetDecoder = StandardCharsets.UTF_8.newDecoder();

    private final PrimitiveTypeEncoding[] _constructors = new PrimitiveTypeEncoding[256];
    private final Map<Object, DescribedTypeConstructor> _dynamicTypeConstructors =
            new HashMap<Object, DescribedTypeConstructor>();

    private final Map<Object, FastPathDescribedTypeConstructor<?>> _fastPathTypeConstructors =
        new HashMap<Object, FastPathDescribedTypeConstructor<?>>();

    public DecoderImpl()
    {
    }

    DecoderImpl(final ByteBuffer buffer)
    {
        _buffer = new ReadableBuffer.ByteBufferReader(buffer);
    }

    public TypeConstructor<?> peekConstructor()
    {
        _buffer.mark();
        try
        {
            return readConstructor();
        }
        finally
        {
            _buffer.reset();
        }
    }

    @SuppressWarnings("rawtypes")
    public TypeConstructor readConstructor()
    {
        return readConstructor(false);
    }

    @SuppressWarnings("rawtypes")
    public TypeConstructor readConstructor(boolean excludeFastPathConstructors)
    {
        int code = ((int)readRawByte()) & 0xff;
        if(code == EncodingCodes.DESCRIBED_TYPE_INDICATOR)
        {
            final byte encoding = _buffer.get(_buffer.position());
            final Object descriptor;

            if (EncodingCodes.SMALLULONG == encoding || EncodingCodes.ULONG == encoding)
            {
                descriptor = readUnsignedLong(null);
            }
            else if (EncodingCodes.SYM8 == encoding || EncodingCodes.SYM32 == encoding)
            {
                descriptor = readSymbol(null);
            }
            else
            {
                descriptor = readObject();
            }

            if (!excludeFastPathConstructors)
            {
                TypeConstructor<?> fastPathTypeConstructor = _fastPathTypeConstructors.get(descriptor);
                if (fastPathTypeConstructor != null)
                {
                    return fastPathTypeConstructor;
                }
            }

            TypeConstructor<?> nestedEncoding = readConstructor(false);
            DescribedTypeConstructor<?> dtc = _dynamicTypeConstructors.get(descriptor);
            if(dtc == null)
            {
                dtc = new DescribedTypeConstructor()
                {
                    @Override
                    public DescribedType newInstance(final Object described)
                    {
                        return new UnknownDescribedType(descriptor, described);
                    }

                    @Override
                    public Class<?> getTypeClass()
                    {
                        return UnknownDescribedType.class;
                    }
                };
                register(descriptor, dtc);
            }
            return new DynamicTypeConstructor(dtc, nestedEncoding);
        }
        else
        {
            return _constructors[code];
        }
    }

    @Override
    public void register(final Object descriptor, final FastPathDescribedTypeConstructor<?> btc)
    {
        _fastPathTypeConstructors.put(descriptor, btc);
    }

    @Override
    public void register(final Object descriptor, final DescribedTypeConstructor dtc)
    {
        // Allow external type constructors to replace the built-in instances.
        _fastPathTypeConstructors.remove(descriptor);
        _dynamicTypeConstructors.put(descriptor, dtc);
    }

    private ClassCastException unexpectedType(final Object val, Class clazz)
    {
        return new ClassCastException("Unexpected type "
                                      + val.getClass().getName()
                                      + ". Expected "
                                      + clazz.getName() +".");
    }


    @Override
    public Boolean readBoolean()
    {
        return readBoolean(null);
    }

    @Override
    public Boolean readBoolean(final Boolean defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.BOOLEAN_TRUE:
                return Boolean.TRUE;
            case EncodingCodes.BOOLEAN_FALSE:
                return Boolean.FALSE;
            case EncodingCodes.BOOLEAN:
                return readRawByte() == 0 ? Boolean.FALSE : Boolean.TRUE;
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Boolean type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public boolean readBoolean(final boolean defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.BOOLEAN_TRUE:
                return true;
            case EncodingCodes.BOOLEAN_FALSE:
                return false;
            case EncodingCodes.BOOLEAN:
                return readRawByte() != 0;
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Boolean type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Byte readByte()
    {
        return readByte(null);
    }

    @Override
    public Byte readByte(final Byte defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return (Byte) readRawByte();
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public byte readByte(final byte defaultVal)
    {
        TypeConstructor<?> constructor = readConstructor();
        if(constructor instanceof ByteType.ByteEncoding)
        {
            return ((ByteType.ByteEncoding)constructor).readPrimitiveValue();
        }
        else
        {
            Object val = constructor.readValue();
            if(val == null)
            {
                return defaultVal;
            }
            else
            {
                throw unexpectedType(val, Byte.class);
            }
        }
    }

    @Override
    public Short readShort()
    {
        return readShort(null);
    }

    @Override
    public Short readShort(final Short defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.SHORT:
                return Short.valueOf(readRawShort());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public short readShort(final short defaultVal)
    {
        TypeConstructor<?> constructor = readConstructor();
        if(constructor instanceof ShortType.ShortEncoding)
        {
            return ((ShortType.ShortEncoding)constructor).readPrimitiveValue();
        }
        else
        {
            Object val = constructor.readValue();
            if(val == null)
            {
                return defaultVal;
            }
            else
            {
                throw unexpectedType(val, Short.class);
            }
        }
    }

    @Override
    public Integer readInteger()
    {
        return readInteger(null);
    }

    @Override
    public Integer readInteger(final Integer defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.SMALLINT:
                return Integer.valueOf(readRawByte());
            case EncodingCodes.INT:
                return Integer.valueOf(readRawInt());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readInteger(final int defaultVal)
    {
        TypeConstructor<?> constructor = readConstructor();
        if(constructor instanceof IntegerType.IntegerEncoding)
        {
            return ((IntegerType.IntegerEncoding)constructor).readPrimitiveValue();
        }
        else
        {
            Object val = constructor.readValue();
            if(val == null)
            {
                return defaultVal;
            }
            else
            {
                throw unexpectedType(val, Integer.class);
            }
        }
    }

    @Override
    public Long readLong()
    {
        return readLong(null);
    }

    @Override
    public Long readLong(final Long defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.SMALLLONG:
                return Long.valueOf(readRawByte());
            case EncodingCodes.LONG:
                return Long.valueOf(readRawLong());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readLong(final long defaultVal)
    {
        TypeConstructor<?> constructor = readConstructor();
        if(constructor instanceof LongType.LongEncoding)
        {
            return ((LongType.LongEncoding)constructor).readPrimitiveValue();
        }
        else
        {
            Object val = constructor.readValue();
            if(val == null)
            {
                return defaultVal;
            }
            else
            {
                throw unexpectedType(val, Long.class);
            }
        }
    }

    @Override
    public UnsignedByte readUnsignedByte()
    {
        return readUnsignedByte(null);
    }

    @Override
    public UnsignedByte readUnsignedByte(final UnsignedByte defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.UBYTE:
                return UnsignedByte.valueOf(readRawByte());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected UnsignedByte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedShort readUnsignedShort()
    {
        return readUnsignedShort(null);
    }

    @Override
    public UnsignedShort readUnsignedShort(final UnsignedShort defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.USHORT:
                return UnsignedShort.valueOf(readRawShort());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected UnsignedShort type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger()
    {
        return readUnsignedInteger(null);
    }

    @Override
    public UnsignedInteger readUnsignedInteger(final UnsignedInteger defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.UINT0:
                return UnsignedInteger.ZERO;
            case EncodingCodes.SMALLUINT:
                return UnsignedInteger.valueOf(((int) readRawByte()) & 0xff);
            case EncodingCodes.UINT:
                return UnsignedInteger.valueOf(readRawInt());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected UnsignedInteger type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedLong readUnsignedLong()
    {
        return readUnsignedLong(null);
    }

    @Override
    public UnsignedLong readUnsignedLong(final UnsignedLong defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.ULONG0:
                return UnsignedLong.ZERO;
            case EncodingCodes.SMALLULONG:
                return UnsignedLong.valueOf(((long) readRawByte())&0xffl);
            case EncodingCodes.ULONG:
                return UnsignedLong.valueOf(readRawLong());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected UnsignedLong type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Character readCharacter()
    {
        return readCharacter(null);
    }

    @Override
    public Character readCharacter(final Character defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.CHAR:
                return Character.valueOf((char) (readRawInt() & 0xffff));
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Character type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public char readCharacter(final char defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.CHAR:
                return (char) (readRawInt() & 0xffff);
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Character type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Float readFloat()
    {
        return readFloat(null);
    }

    @Override
    public Float readFloat(final Float defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.FLOAT:
                return Float.valueOf(readRawFloat());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Float type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public float readFloat(final float defaultVal)
    {
        TypeConstructor<?> constructor = readConstructor();
        if(constructor instanceof FloatType.FloatEncoding)
        {
            return ((FloatType.FloatEncoding)constructor).readPrimitiveValue();
        }
        else
        {
            Object val = constructor.readValue();
            if(val == null)
            {
                return defaultVal;
            }
            else
            {
                throw unexpectedType(val, Float.class);
            }
        }
    }

    @Override
    public Double readDouble()
    {
        return readDouble(null);
    }

    @Override
    public Double readDouble(final Double defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.DOUBLE:
                return Double.valueOf(readRawDouble());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected Double type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public double readDouble(final double defaultVal)
    {
        TypeConstructor<?> constructor = readConstructor();
        if(constructor instanceof DoubleType.DoubleEncoding)
        {
            return ((DoubleType.DoubleEncoding)constructor).readPrimitiveValue();
        }
        else
        {
            Object val = constructor.readValue();
            if(val == null)
            {
                return defaultVal;
            }
            else
            {
                throw unexpectedType(val, Double.class);
            }
        }
    }

    @Override
    public UUID readUUID()
    {
        return readUUID(null);
    }

    @Override
    public UUID readUUID(final UUID defaultVal)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.UUID:
                return new UUID(readRawLong(), readRawLong());
            case EncodingCodes.NULL:
                return defaultVal;
            default:
                throw new DecodeException("Expected UUID type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal32 readDecimal32()
    {
        return readDecimal32(null);
    }

    @Override
    public Decimal32 readDecimal32(final Decimal32 defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.DECIMAL32:
                return (Decimal32) _constructors[EncodingCodes.DECIMAL32 & 0xff].readValue();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Decimal32 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal64 readDecimal64()
    {
        return readDecimal64(null);
    }

    @Override
    public Decimal64 readDecimal64(final Decimal64 defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.DECIMAL64:
                return (Decimal64) _constructors[EncodingCodes.DECIMAL64 & 0xff].readValue();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Decimal64 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal128 readDecimal128()
    {
        return readDecimal128(null);
    }

    @Override
    public Decimal128 readDecimal128(final Decimal128 defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.DECIMAL128:
                return (Decimal128) _constructors[EncodingCodes.DECIMAL128 & 0xff].readValue();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Decimal128 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Date readTimestamp()
    {
        return readTimestamp(null);
    }

    @Override
    public Date readTimestamp(final Date defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.TIMESTAMP:
                return new Date(readRawLong());
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Timestamp type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Binary readBinary()
    {
        return readBinary(null);
    }

    @Override
    public Binary readBinary(final Binary defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.VBIN8:
                return (Binary) _constructors[EncodingCodes.VBIN8 & 0xff].readValue();
            case EncodingCodes.VBIN32:
                return (Binary) _constructors[EncodingCodes.VBIN32 & 0xff].readValue();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Symbol readSymbol()
    {
        return readSymbol(null);
    }

    @Override
    public Symbol readSymbol(final Symbol defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.SYM8:
                return (Symbol) _constructors[EncodingCodes.SYM8 & 0xff].readValue();
            case EncodingCodes.SYM32:
                return (Symbol) _constructors[EncodingCodes.SYM32 & 0xff].readValue();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Symbol type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public String readString()
    {
        return readString(null);
    }

    @Override
    public String readString(final String defaultValue)
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.STR8:
                return (String) _constructors[EncodingCodes.STR8 & 0xff].readValue();
            case EncodingCodes.STR32:
                return (String) _constructors[EncodingCodes.STR32 & 0xff].readValue();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected String type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List readList()
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.LIST0:
                return Collections.EMPTY_LIST;
            case EncodingCodes.LIST8:
                return (List) _constructors[EncodingCodes.LIST8 & 0xff].readValue();
            case EncodingCodes.LIST32:
                return (List) _constructors[EncodingCodes.LIST32 & 0xff].readValue();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected List type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public <T> void readList(final ListProcessor<T> processor)
    {
        //TODO.
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map readMap()
    {
        byte encodingCode = _buffer.get();

        switch (encodingCode)
        {
            case EncodingCodes.MAP8:
                return (Map) _constructors[EncodingCodes.MAP8 & 0xff].readValue();
            case EncodingCodes.MAP32:
                return (Map) _constructors[EncodingCodes.MAP32 & 0xff].readValue();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Map type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public <T> T[] readArray(final Class<T> clazz)
    {
        return null;  //TODO.
    }

    @Override
    public Object[] readArray()
    {
        return (Object[]) readConstructor().readValue();

    }

    @Override
    public boolean[] readBooleanArray()
    {
        return (boolean[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public byte[] readByteArray()
    {
        return (byte[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public short[] readShortArray()
    {
        return (short[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public int[] readIntegerArray()
    {
        return (int[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public long[] readLongArray()
    {
        return (long[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public float[] readFloatArray()
    {
        return (float[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public double[] readDoubleArray()
    {
        return (double[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public char[] readCharacterArray()
    {
        return (char[]) ((ArrayType.ArrayEncoding)readConstructor()).readValueArray();
    }

    @Override
    public <T> T[] readMultiple(final Class<T> clazz)
    {
        Object val = readObject();
        if(val == null)
        {
            return null;
        }
        else if(val.getClass().isArray())
        {
            if(clazz.isAssignableFrom(val.getClass().getComponentType()))
            {
                return (T[]) val;
            }
            else
            {
                throw unexpectedType(val, Array.newInstance(clazz, 0).getClass());
            }
        }
        else if(clazz.isAssignableFrom(val.getClass()))
        {
            T[] array = (T[]) Array.newInstance(clazz, 1);
            array[0] = (T) val;
            return array;
        }
        else
        {
            throw unexpectedType(val, Array.newInstance(clazz, 0).getClass());
        }
    }

    @Override
    public Object[] readMultiple()
    {
        Object val = readObject();
        if(val == null)
        {
            return null;
        }
        else if(val.getClass().isArray())
        {
            return (Object[]) val;
        }
        else
        {
            Object[] array = (Object[]) Array.newInstance(val.getClass(), 1);
            array[0] = val;
            return array;
        }
    }

    @Override
    public byte[] readByteMultiple()
    {
        return new byte[0];  //TODO.
    }

    @Override
    public short[] readShortMultiple()
    {
        return new short[0];  //TODO.
    }

    @Override
    public int[] readIntegerMultiple()
    {
        return new int[0];  //TODO.
    }

    @Override
    public long[] readLongMultiple()
    {
        return new long[0];  //TODO.
    }

    @Override
    public float[] readFloatMultiple()
    {
        return new float[0];  //TODO.
    }

    @Override
    public double[] readDoubleMultiple()
    {
        return new double[0];  //TODO.
    }

    @Override
    public char[] readCharacterMultiple()
    {
        return new char[0];  //TODO.
    }

    @Override
    public Object readObject()
    {
        boolean arrayType = false;
        byte code = _buffer.get(_buffer.position());
        switch (code)
        {
            case EncodingCodes.ARRAY8:
            case EncodingCodes.ARRAY32:
                arrayType = true;
        }

        TypeConstructor<?> constructor = readConstructor();
        if(constructor== null)
        {
            throw new DecodeException("Unknown constructor");
        }

        if (arrayType) {
            return ((ArrayType.ArrayEncoding)constructor).readValueArray();
        } else {
            return constructor.readValue();
        }
    }

    @Override
    public Object readObject(final Object defaultValue)
    {
        Object val = readObject();
        return val == null ? defaultValue : val;
    }

    <V> void register(PrimitiveType<V> type)
    {
        Collection<? extends PrimitiveTypeEncoding<V>> encodings = type.getAllEncodings();

        for(PrimitiveTypeEncoding<V> encoding : encodings)
        {
            _constructors[((int) encoding.getEncodingCode()) & 0xFF ] = encoding;
        }

    }

    byte readRawByte()
    {
        return _buffer.get();
    }

    int readRawInt()
    {
        return _buffer.getInt();
    }

    long readRawLong()
    {
        return _buffer.getLong();
    }

    short readRawShort()
    {
        return _buffer.getShort();
    }

    float readRawFloat()
    {
        return _buffer.getFloat();
    }

    double readRawDouble()
    {
        return _buffer.getDouble();
    }

    void readRaw(final byte[] data, final int offset, final int length)
    {
        _buffer.get(data, offset, length);
    }

    <V> V readRaw(TypeDecoder<V> decoder, int size)
    {
        final int oldLimit = _buffer.limit();
        final int oldPosition = _buffer.position();

        final V decode;

        try {
            decode = decoder.decode(this, _buffer.limit(_buffer.position() + size));
        } finally {
            _buffer.position(oldPosition + size);
            _buffer.limit(oldLimit);
        }

        return decode;
    }

    @Override
    public void setByteBuffer(final ByteBuffer buffer)
    {
        _buffer = new ReadableBuffer.ByteBufferReader(buffer);
    }

    public ByteBuffer getByteBuffer()
    {
        return _buffer.byteBuffer();
    }

    public void setBuffer(final ReadableBuffer buffer)
    {
        _buffer = buffer;
    }

    public ReadableBuffer getBuffer()
    {
        return _buffer;
    }

    CharsetDecoder getCharsetDecoder()
    {
        return _charsetDecoder;
    }

    interface TypeDecoder<V>
    {
        V decode(DecoderImpl decoder, ReadableBuffer buf);
    }

    private static class UnknownDescribedType implements DescribedType
    {
        private final Object _descriptor;
        private final Object _described;

        public UnknownDescribedType(final Object descriptor, final Object described)
        {
            _descriptor = descriptor;
            _described = described;
        }

        @Override
        public Object getDescriptor()
        {
            return _descriptor;
        }

        @Override
        public Object getDescribed()
        {
            return _described;
        }


        @Override
        public boolean equals(Object obj)
        {

            return obj instanceof DescribedType
                   && _descriptor == null ? ((DescribedType) obj).getDescriptor() == null
                                         : _descriptor.equals(((DescribedType) obj).getDescriptor())
                   && _described == null ?  ((DescribedType) obj).getDescribed() == null
                                         : _described.equals(((DescribedType) obj).getDescribed());

        }

    }

    @Override
    public int getByteBufferRemaining() {
        return _buffer.remaining();
    }
}
