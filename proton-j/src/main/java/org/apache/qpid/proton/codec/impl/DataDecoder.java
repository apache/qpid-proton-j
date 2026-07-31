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

package org.apache.qpid.proton.codec.impl;

import static org.apache.qpid.proton.codec.impl.DataValidationSupport.validateConstructorSize;
import static org.apache.qpid.proton.codec.impl.DataValidationSupport.validateListCount;
import static org.apache.qpid.proton.codec.impl.DataValidationSupport.validateListSize;
import static org.apache.qpid.proton.codec.impl.DataValidationSupport.validateMapCount;
import static org.apache.qpid.proton.codec.impl.DataValidationSupport.validateMapSize;
import static org.apache.qpid.proton.codec.impl.DataValidationSupport.validateSizeInitial;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.apache.qpid.proton.amqp.*;
import org.apache.qpid.proton.codec.Data;
import org.apache.qpid.proton.codec.EncodingCodes;

class DataDecoder
{

    private static final Charset ASCII = Charset.forName("US-ASCII");
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final long AMQP_VALUE_NUMERIC_CODE = 0x0000000000000077L;
    private static final String AMQP_VALUE_SYMBOLIC_NAME = "amqp:amqp-value:*";
    private static final int AMQP_VALUE_SYMBOLIC_NAME_LENGTH = AMQP_VALUE_SYMBOLIC_NAME.length();
    private static final byte[] AMQP_VALUE_SYMBOLIC_NAME_BYTES =AMQP_VALUE_SYMBOLIC_NAME.getBytes(StandardCharsets.US_ASCII);
    private static final Symbol AMQP_VALUE_SYMBOLIC_DESCRIPTOR = Symbol.valueOf(AMQP_VALUE_SYMBOLIC_NAME);
    private static final UnsignedLong AMQP_VALUE_NUMERIC_DESCRIPTOR = UnsignedLong.valueOf(AMQP_VALUE_NUMERIC_CODE);

    private static final TypeConstructor[] _constructors = new TypeConstructor[256];

    static
    {

        _constructors[0x00] = new DescribedTypeConstructor();

        _constructors[0x40] = new NullConstructor();
        _constructors[0x41] = new TrueConstructor();
        _constructors[0x42] = new FalseConstructor();
        _constructors[0x43] = new UInt0Constructor();
        _constructors[0x44] = new ULong0Constructor();
        _constructors[0x45] = new EmptyListConstructor();

        _constructors[0x50] = new UByteConstructor();
        _constructors[0x51] = new ByteConstructor();
        _constructors[0x52] = new SmallUIntConstructor();
        _constructors[0x53] = new SmallULongConstructor();
        _constructors[0x54] = new SmallIntConstructor();
        _constructors[0x55] = new SmallLongConstructor();
        _constructors[0x56] = new BooleanConstructor();

        _constructors[0x60] = new UShortConstructor();
        _constructors[0x61] = new ShortConstructor();

        _constructors[0x70] = new UIntConstructor();
        _constructors[0x71] = new IntConstructor();
        _constructors[0x72] = new FloatConstructor();
        _constructors[0x73] = new CharConstructor();
        _constructors[0x74] = new Decimal32Constructor();

        _constructors[0x80] = new ULongConstructor();
        _constructors[0x81] = new LongConstructor();
        _constructors[0x82] = new DoubleConstructor();
        _constructors[0x83] = new TimestampConstructor();
        _constructors[0x84] = new Decimal64Constructor();

        _constructors[0x94] = new Decimal128Constructor();
        _constructors[0x98] = new UUIDConstructor();

        _constructors[0xa0] = new SmallBinaryConstructor();
        _constructors[0xa1] = new SmallStringConstructor();
        _constructors[0xa3] = new SmallSymbolConstructor();

        _constructors[0xb0] = new BinaryConstructor();
        _constructors[0xb1] = new StringConstructor();
        _constructors[0xb3] = new SymbolConstructor();

        _constructors[0xc0] = new SmallListConstructor();
        _constructors[0xc1] = new SmallMapConstructor();


        _constructors[0xd0] = new ListConstructor();
        _constructors[0xd1] = new MapConstructor();

        _constructors[0xe0] = new SmallArrayConstructor();
        _constructors[0xf0] = new ArrayConstructor();

    }

    private interface TypeConstructor
    {
        Data.DataType getType();

        int size(ByteBuffer b, DataImpl data);

        void parse(ByteBuffer b, DataImpl data);
    }


    static int decode(ByteBuffer b, DataImpl data)
    {
        if(b.hasRemaining())
        {
            int position = b.position();
            TypeConstructor c = readConstructor(b);
            final int size = c.size(b, data);

            if(b.remaining() >= size)
            {
                c.parse(b, data);
                return 1+size;
            }
            else
            {
                b.position(position);
                return -4; // PN_UNDERFLOW
            }
        }
        return 0;
    }

    private static TypeConstructor readDescriptorConstructor(ByteBuffer b)
    {
        final byte type = b.get();
        if (EncodingCodes.SMALLULONG == type || EncodingCodes.ULONG == type || EncodingCodes.SYM8 == type || EncodingCodes.SYM32 == type)
        {
            final int index = type & 0xff;
            TypeConstructor tc = _constructors[index];
            if(tc == null)
            {
                throw new IllegalArgumentException("No constructor for type " + index);
            }

            return tc;
        }
        else
        {
            throw new IllegalArgumentException("Reserved non-ulong/non-symbol described-type descriptors not supported by this decoder");
        }
    }

    private static TypeConstructor readDescribedConstructor(ByteBuffer buf, boolean isAmqpValue)
    {
        final TypeConstructor constructor = readConstructor(buf);

        if (!isAmqpValue && Data.DataType.DESCRIBED == constructor.getType())
        {
            throw new IllegalArgumentException("DescribedType value encoding cannot be another DescribedType, must be a primitive type");
        }

        return constructor;
    }

    private static TypeConstructor readConstructor(ByteBuffer b)
    {
        int index = b.get() & 0xff;
        TypeConstructor tc = _constructors[index];
        if(tc == null)
        {
            throw new IllegalArgumentException("No constructor for type " + index);
        }
        return tc;
    }

    private static class NullConstructor extends Fixed0SizeConstructor
    {
        @Override
        public Data.DataType getType()
        {
            return Data.DataType.NULL;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putNull();
        }
    }

    private static class TrueConstructor extends Fixed0SizeConstructor
    {
        @Override
        public Data.DataType getType()
        {
            return Data.DataType.BOOL;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putBoolean(true);
        }
    }


    private static class FalseConstructor extends Fixed0SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.BOOL;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putBoolean(false);
        }
    }

    private static class UInt0Constructor extends Fixed0SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.UINT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedInteger(UnsignedInteger.ZERO);
        }
    }

    private static class ULong0Constructor extends Fixed0SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.ULONG;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedLong(UnsignedLong.ZERO);
        }
    }


    private static class EmptyListConstructor extends Fixed0SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.LIST;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                data.putList();
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }
    }


    private static abstract class Fixed0SizeConstructor implements TypeConstructor
    {
        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            return 0;
        }
    }

    private static abstract class Fixed1SizeConstructor implements TypeConstructor
    {
        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            return 1;
        }
    }

    private static abstract class Fixed2SizeConstructor implements TypeConstructor
    {
        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            return 2;
        }
    }

    private static abstract class Fixed4SizeConstructor implements TypeConstructor
    {
        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            return 4;
        }
    }

    private static abstract class Fixed8SizeConstructor implements TypeConstructor
    {
        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            return 8;
        }
    }

    private static abstract class Fixed16SizeConstructor implements TypeConstructor
    {
        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            return 16;
        }
    }

    private static class UByteConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.UBYTE;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedByte(UnsignedByte.valueOf(b.get()));
        }
    }

    private static class ByteConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.BYTE;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putByte(b.get());
        }
    }

    private static class SmallUIntConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.UINT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedInteger(UnsignedInteger.valueOf(((int) b.get()) & 0xff));
        }
    }

    private static class SmallIntConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.INT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putInt(b.get());
        }
    }

    private static class SmallULongConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.ULONG;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedLong(UnsignedLong.valueOf(((int) b.get()) & 0xff));
        }
    }

    private static class SmallLongConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.LONG;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putLong(b.get());
        }
    }

    private static class BooleanConstructor extends Fixed1SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.BOOL;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int i = b.get();
            if(i != 0 && i != 1)
            {
                throw new IllegalArgumentException("Illegal value " + i + " for boolean");
            }
            data.putBoolean(i == 1);
        }
    }

    private static class UShortConstructor extends Fixed2SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.USHORT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedShort(UnsignedShort.valueOf(b.getShort()));
        }
    }

    private static class ShortConstructor extends Fixed2SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.SHORT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putShort(b.getShort());
        }
    }

    private static class UIntConstructor extends Fixed4SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.UINT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedInteger(UnsignedInteger.valueOf(b.getInt()));
        }
    }

    private static class IntConstructor extends Fixed4SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.INT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putInt(b.getInt());
        }
    }

    private static class FloatConstructor extends Fixed4SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.FLOAT;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putFloat(b.getFloat());
        }
    }

    private static class CharConstructor extends Fixed4SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.CHAR;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putChar(b.getInt());
        }
    }

    private static class Decimal32Constructor extends Fixed4SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.DECIMAL32;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putDecimal32(new Decimal32(b.getInt()));
        }
    }

    private static class ULongConstructor extends Fixed8SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.ULONG;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUnsignedLong(UnsignedLong.valueOf(b.getLong()));
        }
    }

    private static class LongConstructor extends Fixed8SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.LONG;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putLong(b.getLong());
        }
    }

    private static class DoubleConstructor extends Fixed8SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.DOUBLE;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putDouble(b.getDouble());
        }
    }

    private static class TimestampConstructor extends Fixed8SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.TIMESTAMP;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putTimestamp(new Date(b.getLong()));
        }
    }

    private static class Decimal64Constructor extends Fixed8SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.DECIMAL64;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putDecimal64(new Decimal64(b.getLong()));
        }
    }

    private static class Decimal128Constructor extends Fixed16SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.DECIMAL128;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putDecimal128(new Decimal128(b.getLong(), b.getLong()));
        }
    }

    private static class UUIDConstructor extends Fixed16SizeConstructor
    {

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.UUID;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putUUID(new UUID(b.getLong(), b.getLong()));
        }
    }

    private static abstract class SmallVariableConstructor implements TypeConstructor
    {

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            int position = b.position();
            if(b.hasRemaining())
            {
                int size = b.get() & 0xff;
                validateTypeSize(size);
                b.position(position);

                return validateConstructorSize(size+1);
            }
            else
            {
                return 1;
            }
        }

        public abstract void validateTypeSize(int size);
    }

    private static abstract class VariableConstructor implements TypeConstructor
    {

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            int position = b.position();
            if(b.remaining()>=4)
            {
                int size = b.getInt();
                validateTypeSize(size);
                b.position(position);

                return validateConstructorSize(size+4);
            }
            else
            {
                return 4;
            }
        }

        public abstract void validateTypeSize(int size);
    }

    private static class SmallBinaryConstructor extends SmallVariableConstructor
    {
        private static final String TYPE = "Binary";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.BINARY;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int size = b.get() & 0xff;
            DataValidationSupport.validateSize(TYPE, size, b.remaining());
            byte[] bytes = new byte[size];
            b.get(bytes);
            data.putBinary(bytes);
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }

    private static class SmallSymbolConstructor extends SmallVariableConstructor
    {
        private static final String TYPE = "Symbol";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.SYMBOL;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int size = b.get() & 0xff;
            DataValidationSupport.validateSize(TYPE, size, b.remaining());
            byte[] bytes = new byte[size];
            b.get(bytes);
            data.putSymbol(Symbol.valueOf(new String(bytes, ASCII)));
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }


    private static class SmallStringConstructor extends SmallVariableConstructor
    {
        private static final String TYPE = "String";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.STRING;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int size = b.get() & 0xff;
            DataValidationSupport.validateSize(TYPE, size, b.remaining());
            byte[] bytes = new byte[size];
            b.get(bytes);
            data.putString(new String(bytes, UTF_8));
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }

    private static class BinaryConstructor extends VariableConstructor
    {
        private static final String TYPE = "Binary";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.BINARY;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int size = b.getInt();
            DataValidationSupport.validateSize(TYPE, size, b.remaining());
            byte[] bytes = new byte[size];
            b.get(bytes);
            data.putBinary(bytes);
        }

        @Override
        public void validateTypeSize(int size) {
            DataValidationSupport.validateSizeInitial(TYPE, size);
        }
    }

    private static class SymbolConstructor extends VariableConstructor
    {
        private static final String TYPE = "Symbol";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.SYMBOL;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int size = b.getInt();
            DataValidationSupport.validateSize(TYPE, size, b.remaining());
            byte[] bytes = new byte[size];
            b.get(bytes);
            data.putSymbol(Symbol.valueOf(new String(bytes, ASCII)));
        }

        @Override
        public void validateTypeSize(int size) {
            DataValidationSupport.validateSizeInitial(TYPE, size);
        }
    }


    private static class StringConstructor extends VariableConstructor
    {
        private static final String TYPE = "String";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.STRING;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            int size = b.getInt();
            DataValidationSupport.validateSize(TYPE, size, b.remaining());
            byte[] bytes = new byte[size];
            b.get(bytes);
            data.putString(new String(bytes, UTF_8));
        }

        @Override
        public void validateTypeSize(int size) {
            DataValidationSupport.validateSizeInitial(TYPE, size);
        }
    }


    private static class SmallListConstructor extends SmallVariableConstructor
    {
        private static final String TYPE = "List";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.LIST;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                int size = b.get() & 0xff;
                validateListSize(size, b.remaining());
                ByteBuffer buf = b.slice();
                buf.limit(size);
                b.position(b.position()+size);
                int count = buf.get() & 0xff;
                validateListCount(size, count, buf.remaining());
                data.putList();
                parseChildren(data, buf, count, TYPE, size);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }


    private static class SmallMapConstructor extends SmallVariableConstructor
    {
        private static final String TYPE = "Map";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.MAP;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                int size = b.get() & 0xff;
                validateMapSize(size, b.remaining());
                ByteBuffer buf = b.slice();
                buf.limit(size);
                b.position(b.position()+size);
                int count = buf.get() & 0xff;
                validateMapCount(size, count, buf.remaining());
                data.putMap();
                parseChildren(data, buf, count, TYPE, size);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }


    private static class ListConstructor extends VariableConstructor
    {
        private static final String TYPE = "List";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.LIST;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                int size = b.getInt();
                validateListSize(size, b.remaining());
                ByteBuffer buf = b.slice();
                buf.limit(size);
                b.position(b.position()+size);
                int count = buf.getInt();
                validateListCount(size, count, buf.remaining());
                data.putList();
                parseChildren(data, buf, count, TYPE, size);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public void validateTypeSize(int size) {
            DataValidationSupport.validateSizeInitial(TYPE, size);
        }
    }


    private static class MapConstructor extends VariableConstructor
    {
        private static final String TYPE = "Map";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.MAP;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                int size = b.getInt();
                validateMapSize(size, b.remaining());
                ByteBuffer buf = b.slice();
                buf.limit(size);
                b.position(b.position()+size);
                int count = buf.getInt();
                validateMapCount(size, count, buf.remaining());
                data.putMap();
                parseChildren(data, buf, count, TYPE, size);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public void validateTypeSize(int size) {
            DataValidationSupport.validateSizeInitial(TYPE, size);
        }
    }


    private static void parseChildren(DataImpl data, ByteBuffer buf, int count, String type, int parentSize)
    {
        data.enter();
        for(int i = 0; i < count; i++)
        {
            TypeConstructor c = readConstructor(buf);
            final int size = c.size(buf, data);
            final int remaining = buf.remaining();
            if(size <= remaining)
            {
                c.parse(buf, data);
            }
            else
            {
                throw new IllegalArgumentException("Malformed data");
            }
        }

        if (buf.position() != parentSize)
        {
            throw new IllegalArgumentException(type + " incorrectly encoded, left " + (buf.remaining()) + " bytes from size indicated " + parentSize);
        }

        data.exit();
    }

    private static class DescribedTypeConstructor implements TypeConstructor
    {
        @Override
        public Data.DataType getType()
        {
            return Data.DataType.DESCRIBED;
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            ByteBuffer buf = b.slice();
            if(buf.hasRemaining())
            {
                TypeConstructor c = readDescriptorConstructor(buf);
                int descriptorConstructorSize = c.size(buf, data);
                long dtConstructorSize = 1L + descriptorConstructorSize; // 1 more for the type code in readDescriptorConstructor

                if(buf.remaining() > descriptorConstructorSize)
                {
                    buf.position(buf.position() + descriptorConstructorSize);

                    final ByteBuffer descriptorCheckBuf = b.slice(); // slice of original buffer b, not buf!
                    final boolean isAmqpValue = isAmqpValueDescriptor(descriptorCheckBuf);

                    c = readDescribedConstructor(buf, isAmqpValue);

                    final boolean countOwnDepth = isAmqpValue && Data.DataType.DESCRIBED.equals(c.getType());
                    if (countOwnDepth)
                    {
                        data.increaseDecodeDepth();
                    }
                    try
                    {
                        int describedConstructorSize = c.size(buf, data);
                        dtConstructorSize += 1L + describedConstructorSize; // 1  more for the type code in readDescribedConstructor
                    }
                    finally
                    {
                        if (countOwnDepth)
                        {
                            data.decreaseDecodeDepth();
                        }
                    }
                }
                else
                {
                    dtConstructorSize += 1; // another 1 more for the described constructor type code not yet read
                }

                if (dtConstructorSize > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("DescribedTypeConstructor total size cannot exceed " + Integer.MAX_VALUE + " but was: " + dtConstructorSize);
                }

                return validateConstructorSize((int) dtConstructorSize);
            }
            else
            {
                return 1;
            }
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.putDescribed();
            data.enter();

            TypeConstructor c = readDescriptorConstructor(b);

            c.parse(b, data);

            Object current = data.getObject();
            boolean isAmqpValue = AMQP_VALUE_NUMERIC_DESCRIPTOR.equals(current) || AMQP_VALUE_SYMBOLIC_DESCRIPTOR.equals(current);

            c = readDescribedConstructor(b, isAmqpValue);

            boolean countOwnDepth = isAmqpValue && Data.DataType.DESCRIBED.equals(c.getType());
            if (countOwnDepth)
            {
                data.increaseDecodeDepth();
            }
            try
            {
                c.parse(b, data);
                data.exit();
            }
            finally
            {
                if (countOwnDepth)
                {
                    data.decreaseDecodeDepth();
                }
            }
        }
    }

    private static class SmallArrayConstructor extends SmallVariableConstructor
    {
        private static final String TYPE = "Array";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.ARRAY;
        }

        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                int size = b.get() & 0xff;
                validateArraySizeInitial(size, b.remaining());
                ByteBuffer buf = b.slice();
                buf.limit(size);
                b.position(b.position()+size);
                int count = buf.get() & 0xff;
                validateArrayCountInitial(count);

                parseArray(data, buf, count, size);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }

    private static class ArrayConstructor extends VariableConstructor
    {
        private static final String TYPE = "Array";

        @Override
        public Data.DataType getType()
        {
            return Data.DataType.ARRAY;
        }


        @Override
        public void parse(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                int size = b.getInt();
                validateArraySizeInitial(size, b.remaining());
                ByteBuffer buf = b.slice();
                buf.limit(size);
                b.position(b.position()+size);
                int count = buf.getInt();
                validateArrayCountInitial(count);

                parseArray(data, buf, count, size);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public int size(ByteBuffer b, DataImpl data)
        {
            data.increaseDecodeDepth();
            try
            {
                return super.size(b, data);
            }
            finally
            {
                data.decreaseDecodeDepth();
            }
        }

        @Override
        public void validateTypeSize(int size) {
            validateSizeInitial(TYPE, size);
        }
    }

    private static void parseArray(DataImpl data, ByteBuffer buf, int count, int size)
    {
        // element constructor
        byte type = buf.get();
        boolean isDescribed = isTypeCodeDescribedTypeIndicator(type);
        int descriptorPosition = buf.position();
        if(isDescribed)
        {
            TypeConstructor descriptorTc = readDescriptorConstructor(buf);
            buf.position(buf.position() + descriptorTc.size(buf, data));
            type = buf.get();
            if(isTypeCodeDescribedTypeIndicator(type))
            {
                throw new IllegalArgumentException("DescribedType value encoding cannot be another DescribedType, must be a primitive type");
            }
        }

        // read array / elements
        TypeConstructor tc = _constructors[type&0xff];
        checkArraySizeCountConstructor(data, buf.remaining(), tc, count, size);
        data.putArray(isDescribed, tc.getType());
        data.enter();
        if(isDescribed)
        {
            int position = buf.position();
            buf.position(descriptorPosition);
            TypeConstructor descriptorTc = readDescriptorConstructor(buf);
            descriptorTc.parse(buf, data);
            buf.position(position);
        }
        for(int i = 0; i<count; i++)
        {
            tc.parse(buf, data);
        }

        if (buf.position() != size)
        {
            throw new IllegalArgumentException("Array incorrectly encoded, left " + (buf.remaining()) + " bytes from size indicated " + size);
        }

        data.exit();
    }

    private static void validateArraySizeInitial(int size, int remaining)
    {
        if (size < 0)
        {
            throw new IllegalArgumentException("Array size " + size + " cannot be negative");
        }

        if (size > remaining)
        {
            throw new IllegalArgumentException("Array size " + size + " cannot be more than remaining bytes " + remaining);
        }
    }

    private static void validateArrayCountInitial(int count)
    {
        if (count < 0)
        {
            throw new IllegalArgumentException("Array element count " + count + " cannot be negative");
        }
    }

    private static void checkArraySizeCountConstructor(Data data, int remaining, final TypeConstructor constructor, final int count, final int size)
    {
        if(constructor instanceof Fixed0SizeConstructor)
        {
            if (count > data.getZeroWidthElementLimit())
            {
                throw new IllegalArgumentException("Array element count " + count + " is specified to be greater than the allowed 0-width-element limit for arrays ("
                        + data.getZeroWidthElementLimit() + ")");
            }
            else
            {
                // Remaining checks are not valid for these cases, return
                return;
            }
        }

        if (count > size)
        {
            throw new IllegalArgumentException("Array element count " + count + " is specified to be greater than the size of array (" + size + ")");
        }

        if (count > remaining)
        {
            throw new IllegalArgumentException("Array element count " + count + " is specified to be greater than the amount of data available (" + remaining + ")");
        }
    }

    private static boolean isTypeCodeDescribedTypeIndicator(byte type)
    {
        return type == (byte)0x00;
    }

    private static boolean isAmqpValueDescriptor(ByteBuffer buf)
    {
        if(buf.hasRemaining())
        {
            byte type = buf.get();
            switch (type) {
                case EncodingCodes.SMALLULONG:
                    if(buf.hasRemaining())
                    {
                        byte value = buf.get();

                        return value == AMQP_VALUE_NUMERIC_CODE;
                    }
                    break;
                case EncodingCodes.ULONG:
                    if (buf.remaining() >= 8)
                    {
                        long value = buf.getLong();

                        return value == AMQP_VALUE_NUMERIC_CODE;
                    }
                    break;
                case EncodingCodes.SYM8:
                    if(buf.hasRemaining())
                    {
                        int length = buf.get() & 0xFF;

                        return checkSymbolContentIsAmqpValue(buf, length);
                    }
                    break;
                case EncodingCodes.SYM32:
                    if (buf.remaining() >= 4)
                    {
                        int length = buf.getInt();
                        validateSizeInitial("Symbol", length);

                        return checkSymbolContentIsAmqpValue(buf, length);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Reserved non-ulong/non-symbol described-type descriptors not supported by this decoder");
            }
        }

        return false;
    }

    private static boolean checkSymbolContentIsAmqpValue(ByteBuffer buf, int length) {
        if (length == AMQP_VALUE_SYMBOLIC_NAME_LENGTH && length <= buf.remaining()) {
            byte[] bytes = new byte[AMQP_VALUE_SYMBOLIC_NAME_LENGTH];
            buf.get(bytes);

            return Arrays.equals(AMQP_VALUE_SYMBOLIC_NAME_BYTES, bytes);
        }

        return false;
    }
}