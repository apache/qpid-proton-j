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
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.MapType;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathMessageAnnotationsType implements AMQPType<MessageAnnotations>, FastPathDescribedTypeConstructor<MessageAnnotations> {

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(0x0000000000000072L), Symbol.valueOf("amqp:message-annotations:map"),
    };

    private final MessageAnnotationsType annotationsType;

    public FastPathMessageAnnotationsType(EncoderImpl encoder) {
        this.annotationsType = new MessageAnnotationsType(encoder);
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

    @SuppressWarnings("unchecked")
    @Override
    public MessageAnnotations readValue() {
        return new MessageAnnotations(getDecoder().readMap());
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(MessageAnnotations val) {
        WritableBuffer buffer = getEncoder().getBuffer();

        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        getEncoder().writeUnsignedLong(annotationsType.getDescriptor());

        MapType mapType = (MapType) getEncoder().getType(val.getValue());

        mapType.setKeyEncoding(getEncoder().getTypeFromClass(Symbol.class));
        mapType.write(val.getValue());
        mapType.setKeyEncoding(null);
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathMessageAnnotationsType type = new FastPathMessageAnnotationsType(encoder);
        for(Object descriptor : DESCRIPTORS)
        {
            decoder.register(descriptor, type);
        }
        encoder.register(type);
    }
}
