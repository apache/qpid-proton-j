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
package org.apache.qpid.proton.codec.messaging;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.ProtonException;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.ArrayType;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.MapType;
import org.apache.qpid.proton.codec.PrimitiveTypeEncoding;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.SymbolType;
import org.apache.qpid.proton.codec.TypeConstructor;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathMessageAnnotationsType implements AMQPType<MessageAnnotations>, FastPathDescribedTypeConstructor<MessageAnnotations> {

    private static final byte DESCRIPTOR_CODE = 0x72;

    private static final Object[] DESCRIPTORS = {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:message-annotations:map"),
    };

    private final MessageAnnotationsType annotationsType;
    private final SymbolType symbolType;

    public FastPathMessageAnnotationsType(EncoderImpl encoder) {
        this.annotationsType = new MessageAnnotationsType(encoder);
        this.symbolType = (SymbolType) encoder.getTypeFromClass(Symbol.class);
    }

    public EncoderImpl getEncoder() {
        return annotationsType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return annotationsType.getDecoder();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<MessageAnnotations> getTypeClass() {
        return MessageAnnotations.class;
    }

    @Override
    public TypeEncoding<MessageAnnotations> getEncoding(MessageAnnotations val) {
        return annotationsType.getEncoding(val);
    }

    @Override
    public TypeEncoding<MessageAnnotations> getCanonicalEncoding() {
        return annotationsType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<MessageAnnotations>> getAllEncodings() {
        return annotationsType.getAllEncodings();
    }

    @Override
    public MessageAnnotations readValue() {
        DecoderImpl decoder = getDecoder();
        ReadableBuffer buffer = decoder.getBuffer();

        final int size;
        final int count;

        byte encodingCode = buffer.get();

        switch (encodingCode) {
            case EncodingCodes.MAP8:
                size = buffer.get() & 0xFF;
                count = buffer.get() & 0xFF;
                break;
            case EncodingCodes.MAP32:
                size = buffer.getInt();
                count = buffer.getInt();
                break;
            case EncodingCodes.NULL:
                return new MessageAnnotations(null);
            default:
                throw new ProtonException("Expected Map type but found encoding: " + encodingCode);
        }

        if (count > buffer.remaining()) {
            throw new IllegalArgumentException("Map element count "+count+" is specified to be greater than the amount of data available ("+
                                               decoder.getByteBufferRemaining()+")");
        }

        TypeConstructor<?> valueConstructor = null;

        Map<Symbol, Object> map = new LinkedHashMap<>(count);
        for(int i = 0; i < count / 2; i++) {
            Symbol key = decoder.readSymbol(null);
            if (key == null) {
                throw new DecodeException("String key in DeliveryAnnotations cannot be null");
            }

            boolean arrayType = false;
            byte code = buffer.get(buffer.position());
            switch (code)
            {
                case EncodingCodes.ARRAY8:
                case EncodingCodes.ARRAY32:
                    arrayType = true;
            }

            valueConstructor = findNextDecoder(decoder, buffer, valueConstructor);

            final Object value;

            if (arrayType) {
                value = ((ArrayType.ArrayEncoding) valueConstructor).readValueArray();
            } else {
                value = valueConstructor.readValue();
            }

            map.put(key, value);
        }

        return new MessageAnnotations(map);
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(MessageAnnotations val) {
        WritableBuffer buffer = getEncoder().getBuffer();

        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);

        MapType mapType = (MapType) getEncoder().getType(val.getValue());

        mapType.setKeyEncoding(symbolType);
        try {
            mapType.write(val.getValue());
        } finally {
            mapType.setKeyEncoding(null);
        }
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathMessageAnnotationsType type = new FastPathMessageAnnotationsType(encoder);
        for(Object descriptor : DESCRIPTORS) {
            decoder.register(descriptor, type);
        }
        encoder.register(type);
    }

    private static TypeConstructor<?> findNextDecoder(DecoderImpl decoder, ReadableBuffer buffer, TypeConstructor<?> previousConstructor) {
        if (previousConstructor == null) {
            return decoder.readConstructor();
        } else {
            byte encodingCode = buffer.get(buffer.position());
            if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR || !(previousConstructor instanceof PrimitiveTypeEncoding<?>)) {
                previousConstructor = decoder.readConstructor();
            } else {
                PrimitiveTypeEncoding<?> primitiveConstructor = (PrimitiveTypeEncoding<?>) previousConstructor;
                if (encodingCode != primitiveConstructor.getEncodingCode()) {
                    previousConstructor = decoder.readConstructor();
                } else {
                    // consume the encoding code byte for real
                    encodingCode = buffer.get();
                }
            }
        }

        if (previousConstructor == null) {
            throw new DecodeException("Unknown constructor found in Map encoding: ");
        }

        return previousConstructor;
    }
}
