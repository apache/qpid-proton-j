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
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

public class FastPathHeaderType implements AMQPType<Header>, FastPathDescribedTypeConstructor<Header> {

    private static final byte DESCRIPTOR_CODE = 0x70;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:header:list"),
    };

    private final HeaderType headerType;

    public FastPathHeaderType(EncoderImpl encoder) {
        this.headerType = new HeaderType(encoder);
    }

    public EncoderImpl getEncoder() {
        return headerType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return headerType.getDecoder();
    }

    @Override
    public Header readValue() {
        DecoderImpl decoder = getDecoder();
        ReadableBuffer buffer = decoder.getBuffer();
        byte typeCode = decoder.getBuffer().get();

        @SuppressWarnings("unused")
        int size = 0;
        int count = 0;

        switch (typeCode) {
            case EncodingCodes.LIST0:
                break;
            case EncodingCodes.LIST8:
                size = buffer.get() & 0xff;
                count = buffer.get() & 0xff;
                break;
            case EncodingCodes.LIST32:
                size = buffer.getInt();
                count = buffer.getInt();
                break;
            default:
                throw new DecodeException("Incorrect type found in Header encoding: " + typeCode);
        }

        Header header = new Header();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    header.setDurable(decoder.readBoolean(null));
                    break;
                case 1:
                    header.setPriority(decoder.readUnsignedByte(null));
                    break;
                case 2:
                    header.setTtl(decoder.readUnsignedInteger(null));
                    break;
                case 3:
                    header.setFirstAcquirer(decoder.readBoolean(null));
                    break;
                case 4:
                    header.setDeliveryCount(decoder.readUnsignedInteger(null));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Header encoding");
            }
        }

        return header;
    }

    @Override
    public void skipValue() {
        getDecoder().readConstructor().skipValue();
    }

    @Override
    public boolean encodesJavaPrimitive() {
        return false;
    }

    @Override
    public Class<Header> getTypeClass() {
        return Header.class;
    }

    @Override
    public TypeEncoding<Header> getEncoding(Header header) {
        return headerType.getEncoding(header);
    }

    @Override
    public TypeEncoding<Header> getCanonicalEncoding() {
        return headerType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<Header>> getAllEncodings() {
        return headerType.getAllEncodings();
    }

    @Override
    public void write(Header value) {
        WritableBuffer buffer = getEncoder().getBuffer();
        int count = getElementCount(value);
        byte encodingCode = deduceEncodingCode(value, count);

        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);
        buffer.put(encodingCode);

        // Optimized step, no other data to be written.
        if (encodingCode == EncodingCodes.LIST0) {
            return;
        }

        final int fieldWidth;

        if (encodingCode == EncodingCodes.LIST8) {
            fieldWidth = 1;
        } else {
            fieldWidth = 4;
        }

        int startIndex = buffer.position();

        // Reserve space for the size and write the count of list elements.
        if (fieldWidth == 1) {
            buffer.put((byte) 0);
            buffer.put((byte) count);
        } else {
            buffer.putInt(0);
            buffer.putInt(count);
        }

        // Write the list elements and then compute total size written.
        for (int i = 0; i < count; ++i) {
            writeElement(value, i);
        }

        // Move back and write the size
        int endIndex = buffer.position();
        int writeSize = endIndex - startIndex - fieldWidth;

        buffer.position(startIndex);
        if (fieldWidth == 1) {
            buffer.put((byte) writeSize);
        } else {
            buffer.putInt(writeSize);
        }
        buffer.position(endIndex);
    }

    private void writeElement(Header header, int index) {
        switch (index) {
            case 0:
                getEncoder().writeBoolean(header.getDurable());
                break;
            case 1:
                getEncoder().writeUnsignedByte(header.getPriority());
                break;
            case 2:
                getEncoder().writeUnsignedInteger(header.getTtl());
                break;
            case 3:
                getEncoder().writeBoolean(header.getFirstAcquirer());
                break;
            case 4:
                getEncoder().writeUnsignedInteger(header.getDeliveryCount());
                break;
            default:
                throw new IllegalArgumentException("Unknown Header value index: " + index);
        }
    }

    private int getElementCount(Header header) {
        if (header.getDeliveryCount() != null) {
            return 5;
        } else if (header.getFirstAcquirer() != null) {
            return 4;
        } else if (header.getTtl() != null) {
            return 3;
        } else if (header.getPriority() != null) {
            return 2;
        } else if (header.getDurable() != null) {
            return 1;
        } else {
            return 0;
        }
    }

    private byte deduceEncodingCode(Header value, int elementCount) {
        if (elementCount == 0) {
            return EncodingCodes.LIST0;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathHeaderType type = new FastPathHeaderType(encoder);
        for(Object descriptor : DESCRIPTORS)
        {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
