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

import org.apache.qpid.proton.ProtonException;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathDataType implements AMQPType<Data>, FastPathDescribedTypeConstructor<Data> {

    private static final byte DESCRIPTOR_CODE = 0x75;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:data:binary"),
    };

    private final DataType dataType;

    public FastPathDataType(EncoderImpl encoder) {
        this.dataType = new DataType(encoder);
    }

    public EncoderImpl getEncoder() {
        return dataType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return dataType.getDecoder();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<Data> getTypeClass() {
        return dataType.getTypeClass();
    }

    @Override
    public TypeEncoding<Data> getEncoding(Data val) {
        return dataType.getEncoding(val);
    }

    @Override
    public TypeEncoding<Data> getCanonicalEncoding() {
        return dataType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<Data>> getAllEncodings() {
        return dataType.getAllEncodings();
    }

    @Override
    public Data readValue() {
        ReadableBuffer buffer = getDecoder().getBuffer();
        byte encodingCode = buffer.get();

        int size = 0;

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                size = buffer.get() & 0xFF;
                break;
            case EncodingCodes.VBIN32:
                size = buffer.getInt();
                break;
            case EncodingCodes.NULL:
                return new Data(null);
            default:
                throw new ProtonException("Expected Binary type but found encoding: " + encodingCode);
        }

        if (size > buffer.remaining()) {
            throw new IllegalArgumentException("Binary data size " + size + " is specified to be greater than the " +
                                               "amount of data available ("+ buffer.remaining()+")");
        }

        byte[] data = new byte[size];
        buffer.get(data, 0, size);

        return new Data(new Binary(data));
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(Data data) {
        WritableBuffer buffer = getEncoder().getBuffer();
        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);
        getEncoder().writeBinary(data.getValue());
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathDataType type = new FastPathDataType(encoder);
        for(Object descriptor : DESCRIPTORS) {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
