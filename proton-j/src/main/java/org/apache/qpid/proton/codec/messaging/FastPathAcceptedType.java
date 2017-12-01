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

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathAcceptedType implements AMQPType<Accepted>, FastPathDescribedTypeConstructor<Accepted> {

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(0x0000000000000024L), Symbol.valueOf("amqp:accepted:list"),
    };

    private final AcceptedType acceptedType;

    public FastPathAcceptedType(EncoderImpl encoder) {
        this.acceptedType = new AcceptedType(encoder);
    }

    public EncoderImpl getEncoder() {
        return acceptedType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return acceptedType.getDecoder();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<Accepted> getTypeClass() {
        return Accepted.class;
    }

    @Override
    public TypeEncoding<Accepted> getEncoding(Accepted accepted) {
        return acceptedType.getEncoding(accepted);
    }

    @Override
    public TypeEncoding<Accepted> getCanonicalEncoding() {
        return acceptedType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<Accepted>> getAllEncodings() {
        return acceptedType.getAllEncodings();
    }

    @Override
    public Accepted readValue() {
        DecoderImpl decoder = getDecoder();
        byte typeCode = decoder.getByteBuffer().get();

        switch (typeCode) {
            case EncodingCodes.LIST0:
                break;
            case EncodingCodes.LIST8:
                decoder.getByteBuffer().get();
                decoder.getByteBuffer().get();
                break;
            case EncodingCodes.LIST32:
                decoder.getByteBuffer().getInt();
                decoder.getByteBuffer().getInt();
                break;
            default:
                throw new DecodeException("Incorrect type found in Accepted type encoding: " + typeCode);
        }

        return Accepted.getInstance();
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(Accepted accepted) {
        WritableBuffer buffer = getEncoder().getBuffer();
        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        getEncoder().writeUnsignedLong(acceptedType.getDescriptor());
        buffer.put(EncodingCodes.LIST0);
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathAcceptedType type = new FastPathAcceptedType(encoder);
        for(Object descriptor : DESCRIPTORS) {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
