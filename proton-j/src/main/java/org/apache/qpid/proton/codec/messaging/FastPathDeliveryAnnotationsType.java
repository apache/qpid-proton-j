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
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.MapType;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathDeliveryAnnotationsType implements AMQPType<DeliveryAnnotations>, FastPathDescribedTypeConstructor<DeliveryAnnotations> {

    private static final byte DESCRIPTOR_CODE = 0x71;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:delivery-annotations:map"),
    };

    private final DeliveryAnnotationsType annotationsType;

    public FastPathDeliveryAnnotationsType(EncoderImpl encoder) {
        this.annotationsType = new DeliveryAnnotationsType(encoder);
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
    public Class<DeliveryAnnotations> getTypeClass() {
        return DeliveryAnnotations.class;
    }

    @Override
    public TypeEncoding<DeliveryAnnotations> getEncoding(DeliveryAnnotations val) {
        return annotationsType.getEncoding(val);
    }

    @Override
    public TypeEncoding<DeliveryAnnotations> getCanonicalEncoding() {
        return annotationsType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<DeliveryAnnotations>> getAllEncodings() {
        return annotationsType.getAllEncodings();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DeliveryAnnotations readValue() {
        return new DeliveryAnnotations(getDecoder().readMap());
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(DeliveryAnnotations val) {
        WritableBuffer buffer = getEncoder().getBuffer();

        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);

        MapType mapType = (MapType) getEncoder().getType(val.getValue());

        mapType.setKeyEncoding(getEncoder().getTypeFromClass(Symbol.class));
        mapType.write(val.getValue());
        mapType.setKeyEncoding(null);
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathDeliveryAnnotationsType type = new FastPathDeliveryAnnotationsType(encoder);
        for(Object descriptor : DESCRIPTORS)
        {
            decoder.register(descriptor, type);
        }
        encoder.register(type);
    }
}
