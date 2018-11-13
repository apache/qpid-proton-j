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

import java.nio.ByteBuffer;
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

public final class EncoderImpl implements ByteBufferEncoder
{
    private static final byte DESCRIBED_TYPE_OP = (byte)0;

    private WritableBuffer _buffer;

    private final DecoderImpl _decoder;
    private final Map<Class, AMQPType> _typeRegistry = new HashMap<Class, AMQPType>();
    private Map<Object, AMQPType> _describedDescriptorRegistry = new HashMap<Object, AMQPType>();
    private Map<Class, AMQPType>  _describedTypesClassRegistry = new HashMap<Class, AMQPType>();

    private final NullType              _nullType;
    private final BooleanType           _booleanType;
    private final ByteType              _byteType;
    private final UnsignedByteType      _unsignedByteType;
    private final ShortType             _shortType;
    private final UnsignedShortType     _unsignedShortType;
    private final IntegerType           _integerType;
    private final UnsignedIntegerType   _unsignedIntegerType;
    private final LongType              _longType;
    private final UnsignedLongType      _unsignedLongType;
    private final BigIntegerType        _bigIntegerType;

    private final CharacterType         _characterType;
    private final FloatType             _floatType;
    private final DoubleType            _doubleType;
    private final TimestampType         _timestampType;
    private final UUIDType              _uuidType;

    private final Decimal32Type         _decimal32Type;
    private final Decimal64Type         _decimal64Type;
    private final Decimal128Type        _decimal128Type;

    private final BinaryType            _binaryType;
    private final SymbolType            _symbolType;
    private final StringType            _stringType;

    private final ListType              _listType;
    private final MapType               _mapType;

    private final ArrayType             _arrayType;

    public EncoderImpl(DecoderImpl decoder)
    {
        _decoder                = decoder;
        _nullType               = new NullType(this, decoder);
        _booleanType            = new BooleanType(this, decoder);
        _byteType               = new ByteType(this, decoder);
        _unsignedByteType       = new UnsignedByteType(this, decoder);
        _shortType              = new ShortType(this, decoder);
        _unsignedShortType      = new UnsignedShortType(this, decoder);
        _integerType            = new IntegerType(this, decoder);
        _unsignedIntegerType    = new UnsignedIntegerType(this, decoder);
        _longType               = new LongType(this, decoder);
        _unsignedLongType       = new UnsignedLongType(this, decoder);
        _bigIntegerType         = new BigIntegerType(this, decoder);

        _characterType          = new CharacterType(this, decoder);
        _floatType              = new FloatType(this, decoder);
        _doubleType             = new DoubleType(this, decoder);
        _timestampType          = new TimestampType(this, decoder);
        _uuidType               = new UUIDType(this, decoder);

        _decimal32Type          = new Decimal32Type(this, decoder);
        _decimal64Type          = new Decimal64Type(this, decoder);
        _decimal128Type         = new Decimal128Type(this, decoder);


        _binaryType             = new BinaryType(this, decoder);
        _symbolType             = new SymbolType(this, decoder);
        _stringType             = new StringType(this, decoder);

        _listType               = new ListType(this, decoder);
        _mapType                = new MapType(this, decoder);

        _arrayType              = new ArrayType(this,
                                                decoder,
                                                _booleanType,
                                                _byteType,
                                                _shortType,
                                                _integerType,
                                                _longType,
                                                _floatType,
                                                _doubleType,
                                                _characterType);
    }

    @Override
    public void setByteBuffer(final ByteBuffer buf)
    {
        _buffer = new WritableBuffer.ByteBufferWrapper(buf);
    }

    public void setByteBuffer(final WritableBuffer buf)
    {
        _buffer = buf;
    }

    public WritableBuffer getBuffer()
    {
        return _buffer;
    }

    public DecoderImpl getDecoder()
    {
        return _decoder;
    }

    @Override
    public AMQPType getType(final Object element)
    {
        return getTypeFromClass(element == null ? Void.class : element.getClass(), element);
    }

    public AMQPType getTypeFromClass(final Class clazz)
    {
        return getTypeFromClass(clazz, null);
    }

    private AMQPType<?> getTypeFromClass(final Class<?> clazz, final Object instance)
    {
        AMQPType<?> amqpType = _typeRegistry.get(clazz);
        if(amqpType == null)
        {
            amqpType = deduceTypeFromClass(clazz, instance);
        }

        return amqpType;
    }

    private AMQPType<?> deduceTypeFromClass(final Class<?> clazz, final Object instance) {
        AMQPType<?> amqpType = null;

        if(clazz.isArray())
        {
            amqpType = _arrayType;
        }
        else
        {
            if(List.class.isAssignableFrom(clazz))
            {
                amqpType = _listType;
            }
            else if(Map.class.isAssignableFrom(clazz))
            {
                amqpType = _mapType;
            }
            else if(DescribedType.class.isAssignableFrom(clazz))
            {
                amqpType = _describedTypesClassRegistry.get(clazz);
                if(amqpType == null && instance != null)
                {
                    Object descriptor = ((DescribedType) instance).getDescriptor();
                    amqpType = _describedDescriptorRegistry.get(descriptor);
                    if(amqpType == null)
                    {
                        amqpType = new DynamicDescribedType(this, descriptor);
                        _describedDescriptorRegistry.put(descriptor, amqpType);
                    }
                }

                return amqpType;
            }
        }
        _typeRegistry.put(clazz, amqpType);

        return amqpType;
    }

    @Override
    public <V> void register(AMQPType<V> type)
    {
        register(type.getTypeClass(), type);
    }

    <T> void register(Class<T> clazz, AMQPType<T> type)
    {
        _typeRegistry.put(clazz, type);
    }

    public void registerDescribedType(Class clazz, Object descriptor)
    {
        AMQPType<?> type = _describedDescriptorRegistry.get(descriptor);
        if(type == null)
        {
            type = new DynamicDescribedType(this, descriptor);
            _describedDescriptorRegistry.put(descriptor, type);
        }
        _describedTypesClassRegistry.put(clazz, type);
    }

    @Override
    public void writeNull()
    {
        _buffer.put(EncodingCodes.NULL);
    }

    @Override
    public void writeBoolean(final boolean bool)
    {
        if (bool)
        {
            _buffer.put(EncodingCodes.BOOLEAN_TRUE);
        }
        else
        {
            _buffer.put(EncodingCodes.BOOLEAN_FALSE);
        }
    }

    @Override
    public void writeBoolean(final Boolean bool)
    {
        if(bool == null)
        {
            writeNull();
        }
        else if (Boolean.TRUE.equals(bool))
        {
            _buffer.put(EncodingCodes.BOOLEAN_TRUE);
        }
        else
        {
            _buffer.put(EncodingCodes.BOOLEAN_FALSE);
        }
    }

    @Override
    public void writeUnsignedByte(final UnsignedByte ubyte)
    {
        if(ubyte == null)
        {
            writeNull();
        }
        else
        {
            _unsignedByteType.fastWrite(this, ubyte);
        }
    }

    @Override
    public void writeUnsignedShort(final UnsignedShort ushort)
    {
        if(ushort == null)
        {
            writeNull();
        }
        else
        {
            _unsignedShortType.fastWrite(this, ushort);
        }
    }

    @Override
    public void writeUnsignedInteger(final UnsignedInteger uint)
    {
        if(uint == null)
        {
            writeNull();
        }
        else
        {
            _unsignedIntegerType.fastWrite(this, uint);
        }
    }

    @Override
    public void writeUnsignedLong(final UnsignedLong ulong)
    {
        if(ulong == null)
        {
            writeNull();
        }
        else
        {
            _unsignedLongType.fastWrite(this, ulong);
        }
    }

    @Override
    public void writeByte(final byte b)
    {
        _byteType.write(b);
    }

    @Override
    public void writeByte(final Byte b)
    {
        if(b == null)
        {
            writeNull();
        }
        else
        {
            writeByte(b.byteValue());
        }
    }

    @Override
    public void writeShort(final short s)
    {
        _shortType.write(s);
    }

    @Override
    public void writeShort(final Short s)
    {
        if(s == null)
        {
            writeNull();
        }
        else
        {
            writeShort(s.shortValue());
        }
    }

    @Override
    public void writeInteger(final int i)
    {
        _integerType.write(i);
    }

    @Override
    public void writeInteger(final Integer i)
    {
        if(i == null)
        {
            writeNull();
        }
        else
        {
            writeInteger(i.intValue());
        }
    }

    @Override
    public void writeLong(final long l)
    {
        _longType.write(l);
    }

    @Override
    public void writeLong(final Long l)
    {

        if(l == null)
        {
            writeNull();
        }
        else
        {
            writeLong(l.longValue());
        }
    }

    @Override
    public void writeFloat(final float f)
    {
        _floatType.write(f);
    }

    @Override
    public void writeFloat(final Float f)
    {
        if(f == null)
        {
            writeNull();
        }
        else
        {
            writeFloat(f.floatValue());
        }
    }

    @Override
    public void writeDouble(final double d)
    {
        _doubleType.write(d);
    }

    @Override
    public void writeDouble(final Double d)
    {
        if(d == null)
        {
            writeNull();
        }
        else
        {
            writeDouble(d.doubleValue());
        }
    }

    @Override
    public void writeDecimal32(final Decimal32 d)
    {
        if(d == null)
        {
            writeNull();
        }
        else
        {
            _decimal32Type.write(d);
        }
    }

    @Override
    public void writeDecimal64(final Decimal64 d)
    {
        if(d == null)
        {
            writeNull();
        }
        else
        {
            _decimal64Type.write(d);
        }
    }

    @Override
    public void writeDecimal128(final Decimal128 d)
    {
        if(d == null)
        {
            writeNull();
        }
        else
        {
            _decimal128Type.write(d);
        }
    }

    @Override
    public void writeCharacter(final char c)
    {
        // TODO - java character may be half of a pair, should probably throw exception then
        _characterType.write(c);
    }

    @Override
    public void writeCharacter(final Character c)
    {
        if(c == null)
        {
            writeNull();
        }
        else
        {
            writeCharacter(c.charValue());
        }
    }

    @Override
    public void writeTimestamp(final long timestamp)
    {
        _timestampType.fastWrite(this, timestamp);
    }

    @Override
    public void writeTimestamp(final Date d)
    {
        if(d == null)
        {
            writeNull();
        }
        else
        {
            _timestampType.fastWrite(this, d.getTime());
        }
    }

    @Override
    public void writeUUID(final UUID uuid)
    {
        if(uuid == null)
        {
            writeNull();
        }
        else
        {
            _uuidType.fastWrite(this, uuid);
        }
    }

    @Override
    public void writeBinary(final Binary b)
    {
        if(b == null)
        {
            writeNull();
        }
        else
        {
            _binaryType.fastWrite(this, b);
        }
    }

    @Override
    public void writeString(final String s)
    {
        if(s == null)
        {
            writeNull();
        }
        else
        {
            _stringType.write(s);
        }
    }

    @Override
    public void writeSymbol(final Symbol s)
    {
        if(s == null)
        {
            writeNull();
        }
        else
        {
            _symbolType.fastWrite(this, s);
        }
    }

    @Override
    public void writeList(final List l)
    {
        if(l == null)
        {
            writeNull();
        }
        else
        {
            _listType.write(l);
        }
    }

    @Override
    public void writeMap(final Map m)
    {

        if(m == null)
        {
            writeNull();
        }
        else
        {
            _mapType.write(m);
        }
    }

    @Override
    public void writeDescribedType(final DescribedType d)
    {
        if(d == null)
        {
            writeNull();
        }
        else
        {
            _buffer.put(DESCRIBED_TYPE_OP);
            writeObject(d.getDescriptor());
            writeObject(d.getDescribed());
        }
    }

    @Override
    public void writeArray(final boolean[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final byte[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final short[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final int[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final long[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final float[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final double[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final char[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @Override
    public void writeArray(final Object[] a)
    {
        if(a == null)
        {
            writeNull();
        }
        else
        {
            _arrayType.write(a);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void writeObject(final Object o)
    {
        if (o != null)
        {
            AMQPType type = _typeRegistry.get(o.getClass());

            if(type != null)
            {
                type.write(o);
            }
            else
            {
                writeUnregisteredType(o);
            }
        }
        else
        {
            _buffer.put(EncodingCodes.NULL);
        }
    }

    private void writeUnregisteredType(final Object o)
    {
        if(o.getClass().isArray())
        {
            writeArrayType(o);
        }
        else if(o instanceof List)
        {
            writeList((List<?>)o);
        }
        else if(o instanceof Map)
        {
            writeMap((Map<?, ?>)o);
        }
        else if(o instanceof DescribedType)
        {
            writeDescribedType((DescribedType)o);
        }
        else
        {
            throw new IllegalArgumentException(
                "Do not know how to write Objects of class " + o.getClass().getName());
        }
    }

    private void writeArrayType(Object array) {
        Class<?> componentType = array.getClass().getComponentType();
        if(componentType.isPrimitive())
        {
            if(componentType == Boolean.TYPE)
            {
                writeArray((boolean[])array);
            }
            else if(componentType == Byte.TYPE)
            {
                writeArray((byte[])array);
            }
            else if(componentType == Short.TYPE)
            {
                writeArray((short[])array);
            }
            else if(componentType == Integer.TYPE)
            {
                writeArray((int[])array);
            }
            else if(componentType == Long.TYPE)
            {
                writeArray((long[])array);
            }
            else if(componentType == Float.TYPE)
            {
                writeArray((float[])array);
            }
            else if(componentType == Double.TYPE)
            {
                writeArray((double[])array);
            }
            else if(componentType == Character.TYPE)
            {
                writeArray((char[])array);
            }
            else
            {
                throw new IllegalArgumentException("Cannot write arrays of type " + componentType.getName());
            }
        }
        else
        {
            writeArray((Object[]) array);
        }
    }

    public void writeRaw(final byte b)
    {
        _buffer.put(b);
    }

    void writeRaw(final short s)
    {
        _buffer.putShort(s);
    }

    void writeRaw(final int i)
    {
        _buffer.putInt(i);
    }

    void writeRaw(final long l)
    {
        _buffer.putLong(l);
    }

    void writeRaw(final float f)
    {
        _buffer.putFloat(f);
    }

    void writeRaw(final double d)
    {
        _buffer.putDouble(d);
    }

    void writeRaw(byte[] src, int offset, int length)
    {
        _buffer.put(src, offset, length);
    }

    void writeRaw(final String string)
    {
        _buffer.put(string);
    }

    AMQPType<?> getNullTypeEncoder()
    {
        return _nullType;
    }
}
