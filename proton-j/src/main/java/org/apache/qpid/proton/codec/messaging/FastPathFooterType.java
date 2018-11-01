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
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.MapType;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathFooterType implements AMQPType<Footer>, FastPathDescribedTypeConstructor<Footer> {

    private static final byte DESCRIPTOR_CODE = 0x78;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:footer:map"),
    };

    private final FooterType footerType;

    public FastPathFooterType(EncoderImpl encoder) {
        this.footerType = new FooterType(encoder);
    }

    public EncoderImpl getEncoder() {
        return footerType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return footerType.getDecoder();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<Footer> getTypeClass() {
        return Footer.class;
    }

    @Override
    public TypeEncoding<Footer> getEncoding(Footer val) {
        return footerType.getEncoding(val);
    }

    @Override
    public TypeEncoding<Footer> getCanonicalEncoding() {
        return footerType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<Footer>> getAllEncodings() {
        return footerType.getAllEncodings();
    }

    @Override
    public Footer readValue() {
        return new Footer(getDecoder().readMap());
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public void write(Footer val) {
        WritableBuffer buffer = getEncoder().getBuffer();

        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);

        MapType mapType = (MapType) getEncoder().getType(val.getValue());

        mapType.write(val.getValue());
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathFooterType type = new FastPathFooterType(encoder);
        for(Object descriptor : DESCRIPTORS)
        {
            decoder.register(descriptor, type);
        }
        encoder.register(type);
    }
}
