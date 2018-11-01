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
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathAmqpSequenceType implements AMQPType<AmqpSequence>, FastPathDescribedTypeConstructor<AmqpSequence> {

    private static final byte DESCRIPTOR_CODE = 0x76;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:amqp-sequence:list"),
    };

    private final AmqpSequenceType sequenceType;

    public FastPathAmqpSequenceType(EncoderImpl encoder) {
        this.sequenceType = new AmqpSequenceType(encoder);
    }

    public EncoderImpl getEncoder() {
        return sequenceType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return sequenceType.getDecoder();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<AmqpSequence> getTypeClass() {
        return AmqpSequence.class;
    }

    @Override
    public TypeEncoding<AmqpSequence> getEncoding(AmqpSequence val) {
        return sequenceType.getEncoding(val);
    }

    @Override
    public TypeEncoding<AmqpSequence> getCanonicalEncoding() {
        return sequenceType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<AmqpSequence>> getAllEncodings() {
        return sequenceType.getAllEncodings();
    }

    @Override
    public AmqpSequence readValue() {
        return new AmqpSequence(getDecoder().readList());
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(AmqpSequence sequence) {
        WritableBuffer buffer = getEncoder().getBuffer();
        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);
        getEncoder().writeObject(sequence.getValue());
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathAmqpSequenceType type = new FastPathAmqpSequenceType(encoder);
        for (Object descriptor : DESCRIPTORS) {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
