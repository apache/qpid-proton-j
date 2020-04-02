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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapType extends AbstractPrimitiveType<Map>
{
    private final MapEncoding _mapEncoding;
    private final MapEncoding _shortMapEncoding;
    private EncoderImpl _encoder;

    private AMQPType<?> fixedKeyType;

    private static interface MapEncoding extends PrimitiveTypeEncoding<Map>
    {
        void setValue(Map value, int length);
    }

    MapType(final EncoderImpl encoder, final DecoderImpl decoder)
    {
        _encoder = encoder;
        _mapEncoding = new AllMapEncoding(encoder, decoder);
        _shortMapEncoding = new ShortMapEncoding(encoder, decoder);
        encoder.register(Map.class, this);
        decoder.register(this);
    }

    @Override
    public Class<Map> getTypeClass()
    {
        return Map.class;
    }

    public void setKeyEncoding(AMQPType<?> keyType)
    {
        this.fixedKeyType = keyType;
    }

    @Override
    public MapEncoding getEncoding(final Map val)
    {
        final int calculatedSize = calculateSize(val);
        final MapEncoding encoding = (val.size() > 127 || calculatedSize >= 254)
                                    ? _mapEncoding
                                    : _shortMapEncoding;

        encoding.setValue(val, calculatedSize);
        return encoding;
    }

    private int calculateSize(final Map map)
    {
        int len = 0;

        final Iterator<Map.Entry<?, ?>> iter = map.entrySet().iterator();
        final AMQPType fixedKeyType = this.fixedKeyType;

        // Clear existing fixed key type encoding to prevent application to nested Maps
        setKeyEncoding(null);

        try {
            while (iter.hasNext())
            {
                final Map.Entry<?, ?> element = iter.next();
                TypeEncoding elementEncoding;

                if (fixedKeyType == null)
                {
                    AMQPType keyType = _encoder.getType(element.getKey());
                    if(keyType == null) {
                        throw new IllegalArgumentException(
                                "No encoding is known for map entry key of type: " + element.getKey().getClass().getName());
                    }
                    elementEncoding = keyType.getEncoding(element.getKey());
                }
                else
                {
                    elementEncoding = fixedKeyType.getEncoding(element.getKey());
                }

                len += elementEncoding.getConstructorSize() + elementEncoding.getValueSize(element.getKey());

                AMQPType valueType = _encoder.getType(element.getValue());
                if(valueType == null) {
                    throw new IllegalArgumentException(
                            "No encoding is known for map entry value of type: " + element.getValue().getClass().getName());
                }

                elementEncoding = valueType.getEncoding(element.getValue());
                len += elementEncoding.getConstructorSize() + elementEncoding.getValueSize(element.getValue());
            }
        } finally {
            // Reset Existing key type encoding for later encode step or reuse until cleared by caller
            setKeyEncoding(fixedKeyType);
        }

        return len;
    }

    private static TypeConstructor<?> findNextDecoder(DecoderImpl decoder, ReadableBuffer buffer, TypeConstructor<?> previousConstructor)
    {
        if (previousConstructor == null)
        {
            return decoder.readConstructor();
        }
        else
        {
            byte encodingCode = buffer.get(buffer.position());
            if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR || !(previousConstructor instanceof PrimitiveTypeEncoding<?>))
            {
                return decoder.readConstructor();
            }
            else
            {
                final PrimitiveTypeEncoding<?> primitiveConstructor = (PrimitiveTypeEncoding<?>) previousConstructor;
                if (encodingCode != primitiveConstructor.getEncodingCode())
                {
                    return decoder.readConstructor();
                }
                else
                {
                    // consume the encoding code byte for real
                    encodingCode = buffer.get();
                }
            }
        }

        return previousConstructor;
    }

    @Override
    public MapEncoding getCanonicalEncoding()
    {
        return _mapEncoding;
    }

    @Override
    public Collection<MapEncoding> getAllEncodings()
    {
        return Arrays.asList(_shortMapEncoding, _mapEncoding);
    }

    private class AllMapEncoding
            extends LargeFloatingSizePrimitiveTypeEncoding<Map>
            implements MapEncoding
    {

        private Map _value;
        private int _length;

        public AllMapEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final Map map)
        {
            getEncoder().getBuffer().ensureRemaining(getSizeBytes() + getEncodedValueSize(map));
            getEncoder().writeRaw(2 * map.size());

            final Iterator<Map.Entry> iter = map.entrySet().iterator();
            final AMQPType fixedKeyType = MapType.this.fixedKeyType;

            // Clear existing fixed key type encoding to prevent application to nested Maps
            setKeyEncoding(null);

            try {
                while (iter.hasNext())
                {
                    final Map.Entry<?, ?> element = iter.next();
                    TypeEncoding elementEncoding;

                    if (fixedKeyType == null)
                    {
                        elementEncoding = _encoder.getType(element.getKey()).getEncoding(element.getKey());
                    }
                    else
                    {
                        elementEncoding = fixedKeyType.getEncoding(element.getKey());
                    }

                    elementEncoding.writeConstructor();
                    elementEncoding.writeValue(element.getKey());
                    elementEncoding = getEncoder().getType(element.getValue()).getEncoding(element.getValue());
                    elementEncoding.writeConstructor();
                    elementEncoding.writeValue(element.getValue());
                }
            } finally {
                // Reset Existing key type encoding for later encode step or reuse until cleared by caller
                setKeyEncoding(fixedKeyType);
            }
        }

        @Override
        protected int getEncodedValueSize(final Map val)
        {
            return 4 + ((val == _value) ? _length : calculateSize(val));
        }

        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.MAP32;
        }

        @Override
        public MapType getType()
        {
            return MapType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<Map> encoding)
        {
            return (getType() == encoding.getType());
        }

        @Override
        public Map readValue()
        {
            final DecoderImpl decoder = getDecoder();
            final ReadableBuffer buffer = decoder.getBuffer();

            final int size = decoder.readRawInt();
            // todo - limit the decoder with size
            final int count = decoder.readRawInt();
            if (count > decoder.getByteBufferRemaining()) {
                throw new IllegalArgumentException("Map element count "+count+" is specified to be greater than the amount of data available ("+
                                                   decoder.getByteBufferRemaining()+")");
            }

            TypeConstructor<?> keyConstructor = null;
            TypeConstructor<?> valueConstructor = null;

            Map<Object, Object> map = new LinkedHashMap<>(count);
            for(int i = 0; i < count / 2; i++)
            {
                keyConstructor = findNextDecoder(decoder, buffer, keyConstructor);
                if(keyConstructor == null)
                {
                    throw new DecodeException("Unknown constructor");
                }

                final Object key = keyConstructor.readValue();

                boolean arrayType = false;
                final byte code = buffer.get(buffer.position());
                switch (code)
                {
                    case EncodingCodes.ARRAY8:
                    case EncodingCodes.ARRAY32:
                        arrayType = true;
                }

                valueConstructor = findNextDecoder(decoder, buffer, valueConstructor);
                if (valueConstructor == null)
                {
                    throw new DecodeException("Unknown constructor");
                }

                final Object value;

                if (arrayType)
                {
                    value = ((ArrayType.ArrayEncoding) valueConstructor).readValueArray();
                }
                else
                {
                    value = valueConstructor.readValue();
                }

                map.put(key, value);
            }

            return map;
        }

        @Override
        public void skipValue()
        {
            final DecoderImpl decoder = getDecoder();
            final ReadableBuffer buffer = decoder.getBuffer();
            final int size = decoder.readRawInt();
            buffer.position(buffer.position() + size);
        }

        @Override
        public void setValue(final Map value, final int length)
        {
            _value = value;
            _length = length;
        }
    }

    private class ShortMapEncoding
            extends SmallFloatingSizePrimitiveTypeEncoding<Map>
            implements MapEncoding
    {
        private Map _value;
        private int _length;

        public ShortMapEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final Map map)
        {
            getEncoder().getBuffer().ensureRemaining(getSizeBytes() + getEncodedValueSize(map));
            getEncoder().writeRaw((byte)(2 * map.size()));

            final Iterator<Map.Entry> iter = map.entrySet().iterator();
            final AMQPType fixedKeyType = MapType.this.fixedKeyType;

            // Clear existing fixed key type encoding to prevent application to nested Maps
            setKeyEncoding(null);

            try {
                while (iter.hasNext())
                {
                    final Map.Entry<?, ?> element = iter.next();
                    TypeEncoding elementEncoding;

                    if (fixedKeyType == null)
                    {
                        elementEncoding = _encoder.getType(element.getKey()).getEncoding(element.getKey());
                    }
                    else
                    {
                        elementEncoding = fixedKeyType.getEncoding(element.getKey());
                    }

                    elementEncoding.writeConstructor();
                    elementEncoding.writeValue(element.getKey());
                    elementEncoding = getEncoder().getType(element.getValue()).getEncoding(element.getValue());
                    elementEncoding.writeConstructor();
                    elementEncoding.writeValue(element.getValue());
                }
            } finally {
                // Reset Existing key type encoding for later encode step or reuse until cleared by caller
                setKeyEncoding(fixedKeyType);
            }
        }

        @Override
        protected int getEncodedValueSize(final Map val)
        {
            return 1 + ((val == _value) ? _length : calculateSize(val));
        }

        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.MAP8;
        }

        @Override
        public MapType getType()
        {
            return MapType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<Map> encoder)
        {
            return encoder == this;
        }

        @Override
        public Map readValue()
        {
            final DecoderImpl decoder = getDecoder();
            final ReadableBuffer buffer = decoder.getBuffer();

            final int size = (decoder.readRawByte()) & 0xff;
            // todo - limit the decoder with size
            final int count = (decoder.readRawByte()) & 0xff;

            TypeConstructor<?> keyConstructor = null;
            TypeConstructor<?> valueConstructor = null;

            final Map<Object, Object> map = new LinkedHashMap<>(count);
            for(int i = 0; i < count / 2; i++)
            {
                keyConstructor = findNextDecoder(decoder, buffer, keyConstructor);
                if(keyConstructor == null)
                {
                    throw new DecodeException("Unknown constructor");
                }

                final Object key = keyConstructor.readValue();

                boolean arrayType = false;
                final byte code = buffer.get(buffer.position());
                switch (code)
                {
                    case EncodingCodes.ARRAY8:
                    case EncodingCodes.ARRAY32:
                        arrayType = true;
                }

                valueConstructor = findNextDecoder(decoder, buffer, valueConstructor);
                if(valueConstructor== null)
                {
                    throw new DecodeException("Unknown constructor");
                }

                final Object value;

                if (arrayType)
                {
                    value = ((ArrayType.ArrayEncoding) valueConstructor).readValueArray();
                }
                else
                {
                    value = valueConstructor.readValue();
                }

                map.put(key, value);
            }

            return map;
        }

        @Override
        public void skipValue()
        {
            final DecoderImpl decoder = getDecoder();
            final ReadableBuffer buffer = decoder.getBuffer();
            final int size = ((int)decoder.readRawByte()) & 0xff;
            buffer.position(buffer.position() + size);
        }

        @Override
        public void setValue(final Map value, final int length)
        {
            _value = value;
            _length = length;
        }
    }
}
