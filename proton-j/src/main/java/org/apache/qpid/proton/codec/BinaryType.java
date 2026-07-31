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

import java.util.Arrays;
import java.util.Collection;

import org.apache.qpid.proton.amqp.Binary;

public class BinaryType extends AbstractPrimitiveType<Binary>
{
    private static final String TYPE_NAME = "binary";

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

    @Override
    public Class<Binary> getTypeClass()
    {
        return Binary.class;
    }

    @Override
    public BinaryEncoding getEncoding(final Binary val)
    {
        return val.getLength() <= 255 ? _shortBinaryEncoding : _binaryEncoding;
    }

    @Override
    public BinaryEncoding getCanonicalEncoding()
    {
        return _binaryEncoding;
    }

    @Override
    public Collection<BinaryEncoding> getAllEncodings()
    {
        return Arrays.asList(_shortBinaryEncoding, _binaryEncoding);
    }

    public void fastWrite(EncoderImpl encoder, Binary binary)
    {
        if (binary.getLength() <= 255)
        {
            // Reserve size of body + type encoding and single byte size
            encoder.getBuffer().ensureRemaining(2 + binary.getLength());
            encoder.writeRaw(EncodingCodes.VBIN8);
            encoder.writeRaw((byte) binary.getLength());
            encoder.writeRaw(binary.getArray(), binary.getArrayOffset(), binary.getLength());
        }
        else
        {
            // Reserve size of body + type encoding and four byte size
            encoder.getBuffer().ensureRemaining(5 + binary.getLength());
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
            getEncoder().getBuffer().ensureRemaining(val.getLength());
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

        @Override
        public BinaryType getType()
        {
            return BinaryType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<Binary> encoding)
        {
            return (getType() == encoding.getType());
        }

        @Override
        public Binary readValue()
        {
            final DecoderImpl decoder = getDecoder();
            final int size = decoder.readRawInt();

            ValidationSupport.validateLength(TYPE_NAME, size, decoder.getByteBufferRemaining());

            final byte[] data = new byte[size];
            decoder.readRaw(data, 0, size);
            return new Binary(data);
        }

        @Override
        public void skipValue()
        {
            final DecoderImpl decoder = getDecoder();
            final ReadableBuffer buffer = decoder.getBuffer();
            final int size = decoder.readRawInt();

            ValidationSupport.validateLength(TYPE_NAME, size, buffer.remaining());

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
            getEncoder().getBuffer().ensureRemaining(val.getLength());
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

        @Override
        public BinaryType getType()
        {
            return BinaryType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<Binary> encoder)
        {
            return encoder == this;
        }

        @Override
        public Binary readValue()
        {
            final DecoderImpl decoder = getDecoder();
            final int size = ((int) decoder.readRawByte()) & 0xff;

            ValidationSupport.validateLength(TYPE_NAME, size, decoder.getByteBufferRemaining());

            final byte[] data = new byte[size];
            decoder.readRaw(data, 0, size);
            return new Binary(data);
        }

        @Override
        public void skipValue()
        {
            final DecoderImpl decoder = getDecoder();
            final ReadableBuffer buffer = decoder.getBuffer();
            final int size = ((int) decoder.readRawByte()) & 0xff;

            ValidationSupport.validateLength(TYPE_NAME, size, buffer.remaining());

            buffer.position(buffer.position() + size);
        }
    }
}
