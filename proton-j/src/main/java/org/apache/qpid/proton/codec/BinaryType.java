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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

public class BinaryType extends AbstractPrimitiveType<Binary>
{
    private final BinaryEncoding _binaryEncoding;
    private final BinaryEncoding _shortBinaryEncoding;

    private static interface BinaryEncoding extends PrimitiveTypeEncoding<Binary>
    {

    }

    BinaryType(final EncoderImpl encoder, final DecoderImpl decoder)
    {
        _binaryEncoding = new LongBinaryEncoding(encoder, decoder);
        _shortBinaryEncoding = new ShortBinaryEncoding(encoder, decoder);
        encoder.register(Binary.class, this);
        decoder.register(this);
    }

    public Class<Binary> getTypeClass()
    {
        return Binary.class;
    }

    public BinaryEncoding getEncoding(final Binary val)
    {
        return val.getLength() <= 255 ? _shortBinaryEncoding : _binaryEncoding;
    }

    public BinaryEncoding getCanonicalEncoding()
    {
        return _binaryEncoding;
    }

    public Collection<BinaryEncoding> getAllEncodings()
    {
        return Arrays.asList(_shortBinaryEncoding, _binaryEncoding);
    }

    public void fastWrite(EncoderImpl encoder, Binary binary)
    {
        if (binary.getLength() <= 255)
        {
            encoder.writeRaw(EncodingCodes.VBIN8);
            encoder.writeRaw((byte) binary.getLength());
            encoder.writeRaw(binary.getArray(), binary.getArrayOffset(), binary.getLength());
        }
        else
        {
            encoder.writeRaw(EncodingCodes.VBIN32);
            encoder.writeRaw(binary.getLength());
            encoder.writeRaw(binary.getArray(), binary.getArrayOffset(), binary.getLength());
        }
    }

    private class LongBinaryEncoding
            extends LargeFloatingSizePrimitiveTypeEncoding<Binary>
            implements BinaryEncoding
    {

        public LongBinaryEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final Binary val)
        {
            getEncoder().writeRaw(val.getArray(), val.getArrayOffset(), val.getLength());
        }

        @Override
        protected int getEncodedValueSize(final Binary val)
        {
            return val.getLength();
        }


        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.VBIN32;
        }

        public BinaryType getType()
        {
            return BinaryType.this;
        }

        public boolean encodesSuperset(final TypeEncoding<Binary> encoding)
        {
            return (getType() == encoding.getType());
        }

        public Binary readValue()
        {
            final DecoderImpl decoder = getDecoder();
            int size = decoder.readRawInt();
            if (size > decoder.getByteBufferRemaining()) {
                throw new IllegalArgumentException("Binary data size "+size+" is specified to be greater than the amount of data available ("+
                                                   decoder.getByteBufferRemaining()+")");
            }
            byte[] data = new byte[size];
            decoder.readRaw(data, 0, size);
            return new Binary(data);
        }

        public void skipValue()
        {
            DecoderImpl decoder = getDecoder();
            ByteBuffer buffer = decoder.getByteBuffer();
            int size = decoder.readRawInt();
            buffer.position(buffer.position() + size);
        }
    }

    private class ShortBinaryEncoding
            extends SmallFloatingSizePrimitiveTypeEncoding<Binary>
            implements BinaryEncoding
    {

        public ShortBinaryEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final Binary val)
        {
            getEncoder().writeRaw(val.getArray(), val.getArrayOffset(), val.getLength());
        }

        @Override
        protected int getEncodedValueSize(final Binary val)
        {
            return val.getLength();
        }


        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.VBIN8;
        }

        public BinaryType getType()
        {
            return BinaryType.this;
        }

        public boolean encodesSuperset(final TypeEncoding<Binary> encoder)
        {
            return encoder == this;
        }

        public Binary readValue()
        {
            int size = ((int)getDecoder().readRawByte()) & 0xff;
            byte[] data = new byte[size];
            getDecoder().readRaw(data, 0, size);
            return new Binary(data);
        }

        public void skipValue()
        {
            ByteBuffer buffer = getDecoder().getByteBuffer();
            int size = ((int)getDecoder().readRawByte()) & 0xff;
            buffer.position(buffer.position() + size);
        }
    }
}
