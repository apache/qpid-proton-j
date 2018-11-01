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
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathAmqpValueType implements AMQPType<AmqpValue>, FastPathDescribedTypeConstructor<AmqpValue> {

    private static final byte DESCRIPTOR_CODE = 0x77;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:amqp-value:*"),
    };

    private final AmqpValueType valueType;

    public FastPathAmqpValueType(EncoderImpl encoder) {
        this.valueType = new AmqpValueType(encoder);
    }

    public EncoderImpl getEncoder() {
        return valueType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return valueType.getDecoder();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<AmqpValue> getTypeClass() {
        return AmqpValue.class;
    }

    @Override
    public TypeEncoding<AmqpValue> getEncoding(AmqpValue value) {
        return valueType.getEncoding(value);
    }

    @Override
    public TypeEncoding<AmqpValue> getCanonicalEncoding() {
        return valueType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<AmqpValue>> getAllEncodings() {
        return valueType.getAllEncodings();
    }

    @Override
    public AmqpValue readValue() {
        return new AmqpValue(getDecoder().readObject());
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(AmqpValue value) {
        WritableBuffer buffer = getEncoder().getBuffer();
        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);
        getEncoder().writeObject(value.getValue());
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathAmqpValueType type = new FastPathAmqpValueType(encoder);
        for(Object descriptor : DESCRIPTORS) {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
