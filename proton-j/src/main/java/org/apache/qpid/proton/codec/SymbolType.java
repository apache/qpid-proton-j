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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.proton.amqp.Symbol;


public class SymbolType extends AbstractPrimitiveType<Symbol>
{
    private static final int MAX_SYMBOL_CACHE_SIZE = 4096;
    private static final int MAX_CACHED_SYMBOL_LENGTH = 64;

    private static final int MAX_SASL_SYMBOL_CACHE_SIZE = 64;
    private static final int MAX_CACHED_SASL_SYMBOL_LENGTH = 32;

    private static final ConcurrentMap<ReadableBuffer, Symbol> _symbolCache = new ConcurrentHashMap<ReadableBuffer, Symbol>(2048);
    private static final ConcurrentMap<ReadableBuffer, Symbol> _saslSymbolCache = new ConcurrentHashMap<ReadableBuffer, Symbol>(32);

    private static final String TYPE_NAME = "symbol";

    private static final DecoderImpl.TypeDecoder<Symbol> _symbolDecoder = new AbstractSymbolDecoder(MAX_SYMBOL_CACHE_SIZE, MAX_CACHED_SYMBOL_LENGTH, _symbolCache)
    {
        @Override
        public Symbol createSymbol(final String symbolVal)
        {
            return Symbol.getSymbol(symbolVal);
        }
    };

    private static final DecoderImpl.TypeDecoder<Symbol> _saslSymbolDecoder = new AbstractSymbolDecoder(MAX_SASL_SYMBOL_CACHE_SIZE, MAX_CACHED_SASL_SYMBOL_LENGTH, _saslSymbolCache)
    {
        @Override
        public Symbol createSymbol(final String symbolVal)
        {
            return Symbol.getSaslSymbol(symbolVal);
        }
    };

    private final SymbolEncoding _symbolEncoding;
    private final SymbolEncoding _shortSymbolEncoding;
    private DecoderImpl.TypeDecoder<Symbol> _symbolCreator;

    public static interface SymbolEncoding extends PrimitiveTypeEncoding<Symbol>
    {

    }

    SymbolType(final EncoderImpl encoder, final DecoderImpl decoder)
    {
        this(encoder, decoder, false);
    }

    SymbolType(final EncoderImpl encoder, final DecoderImpl decoder, final boolean sasl)
    {
        _symbolEncoding =  new LongSymbolEncoding(encoder, decoder);
        _shortSymbolEncoding = new ShortSymbolEncoding(encoder, decoder);
        _symbolCreator = sasl ? _saslSymbolDecoder : _symbolDecoder;
        encoder.register(Symbol.class, this);
        decoder.register(this);
    }


    @Override
    public Class<Symbol> getTypeClass()
    {
        return Symbol.class;
    }

    public void fastWrite(EncoderImpl encoder, Symbol symbol)
    {
        if (symbol.length() <= 255)
        {
            // Reserve size of body + type encoding and single byte size
            encoder.getBuffer().ensureRemaining(2 + symbol.length());
            encoder.writeRaw(EncodingCodes.SYM8);
            encoder.writeRaw((byte) symbol.length());
            symbol.writeTo(encoder.getBuffer());
        }
        else
        {
            // Reserve size of body + type encoding and four byte size
            encoder.getBuffer().ensureRemaining(5 + symbol.length());
            encoder.writeRaw(EncodingCodes.SYM32);
            encoder.writeRaw(symbol.length());
            symbol.writeTo(encoder.getBuffer());
        }
    }

    @Override
    public SymbolEncoding getEncoding(final Symbol val)
    {
        return val.length() <= 255 ? _shortSymbolEncoding : _symbolEncoding;
    }

    @Override
    public SymbolEncoding getCanonicalEncoding()
    {
        return _symbolEncoding;
    }

    @Override
    public Collection<SymbolEncoding> getAllEncodings()
    {
        return Arrays.asList(_shortSymbolEncoding, _symbolEncoding);
    }

    private class LongSymbolEncoding
            extends LargeFloatingSizePrimitiveTypeEncoding<Symbol>
            implements SymbolEncoding
    {

        public LongSymbolEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final Symbol val)
        {
            getEncoder().getBuffer().ensureRemaining(getEncodedValueSize(val));
            val.writeTo(getEncoder().getBuffer());
        }

        @Override
        protected int getEncodedValueSize(final Symbol val)
        {
            return val.length();
        }

        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.SYM32;
        }

        @Override
        public SymbolType getType()
        {
            return SymbolType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<Symbol> encoding)
        {
            return (getType() == encoding.getType());
        }

        @Override
        public Symbol readValue()
        {
            DecoderImpl decoder = getDecoder();
            int size = decoder.readRawInt();

            return decoder.readRaw(_symbolCreator, size, TYPE_NAME);
        }

        @Override
        public void skipValue()
        {
            DecoderImpl decoder = getDecoder();
            ReadableBuffer buffer = decoder.getBuffer();
            int size = decoder.readRawInt();
            ValidationSupport.validateLength(TYPE_NAME, size, buffer.remaining());
            buffer.position(buffer.position() + size);
        }
    }

    private class ShortSymbolEncoding
            extends SmallFloatingSizePrimitiveTypeEncoding<Symbol>
            implements SymbolEncoding
    {

        public ShortSymbolEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final Symbol val)
        {
            getEncoder().getBuffer().ensureRemaining(getEncodedValueSize(val));
            val.writeTo(getEncoder().getBuffer());
        }

        @Override
        protected int getEncodedValueSize(final Symbol val)
        {
            return val.length();
        }

        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.SYM8;
        }

        @Override
        public SymbolType getType()
        {
            return SymbolType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<Symbol> encoder)
        {
            return encoder == this;
        }

        @Override
        public Symbol readValue()
        {
            DecoderImpl decoder = getDecoder();
            int size = ((int)decoder.readRawByte()) & 0xff;

            return decoder.readRaw(_symbolCreator, size, TYPE_NAME);
        }

        @Override
        public void skipValue()
        {
            DecoderImpl decoder = getDecoder();
            ReadableBuffer buffer = decoder.getBuffer();
            int size = ((int)decoder.readRawByte()) & 0xff;
            ValidationSupport.validateLength(TYPE_NAME, size, buffer.remaining());
            buffer.position(buffer.position() + size);
        }
    }
}
