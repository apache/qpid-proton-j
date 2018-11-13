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
import org.apache.qpid.proton.amqp.messaging.Properties;
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

public class FastPathPropertiesType implements AMQPType<Properties>, FastPathDescribedTypeConstructor<Properties> {

    private static final byte DESCRIPTOR_CODE = 0x73;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:properties:list"),
    };

    private final PropertiesType propertiesType;

    public FastPathPropertiesType(EncoderImpl encoder) {
        this.propertiesType = new PropertiesType(encoder);
    }

    public EncoderImpl getEncoder() {
        return propertiesType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return propertiesType.getDecoder();
    }

    @Override
    public Properties readValue() {
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
                throw new DecodeException("Incorrect type found in Properties encoding: " + typeCode);
        }

        Properties properties = new Properties();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    properties.setMessageId(decoder.readObject());
                    break;
                case 1:
                    properties.setUserId(decoder.readBinary(null));
                    break;
                case 2:
                    properties.setTo(decoder.readString(null));
                    break;
                case 3:
                    properties.setSubject(decoder.readString(null));
                    break;
                case 4:
                    properties.setReplyTo(decoder.readString(null));
                    break;
                case 5:
                    properties.setCorrelationId(decoder.readObject());
                    break;
                case 6:
                    properties.setContentType(decoder.readSymbol(null));
                    break;
                case 7:
                    properties.setContentEncoding(decoder.readSymbol(null));
                    break;
                case 8:
                    properties.setAbsoluteExpiryTime(decoder.readTimestamp(null));
                    break;
                case 9:
                    properties.setCreationTime(decoder.readTimestamp(null));
                    break;
                case 10:
                    properties.setGroupId(decoder.readString(null));
                    break;
                case 11:
                    properties.setGroupSequence(decoder.readUnsignedInteger(null));
                    break;
                case 12:
                    properties.setReplyToGroupId(decoder.readString(null));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Properties encoding");
            }
        }

        return properties;
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
    public Class<Properties> getTypeClass() {
        return Properties.class;
    }

    @Override
    public TypeEncoding<Properties> getEncoding(Properties properties) {
        return propertiesType.getEncoding(properties);
    }

    @Override
    public TypeEncoding<Properties> getCanonicalEncoding() {
        return propertiesType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<Properties>> getAllEncodings() {
        return propertiesType.getAllEncodings();
    }

    @Override
    public void write(Properties value) {
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

    private byte deduceEncodingCode(Properties value, int elementCount) {
        if (elementCount == 0) {
            return EncodingCodes.LIST0;
        } else {
            return EncodingCodes.LIST32;
        }
    }

    private void writeElement(Properties properties, int index) {
        switch (index) {
            case 0:
                getEncoder().writeObject(properties.getMessageId());
                break;
            case 1:
                getEncoder().writeBinary(properties.getUserId());
                break;
            case 2:
                getEncoder().writeString(properties.getTo());
                break;
            case 3:
                getEncoder().writeString(properties.getSubject());
                break;
            case 4:
                getEncoder().writeString(properties.getReplyTo());
                break;
            case 5:
                getEncoder().writeObject(properties.getCorrelationId());
                break;
            case 6:
                getEncoder().writeSymbol(properties.getContentType());
                break;
            case 7:
                getEncoder().writeSymbol(properties.getContentEncoding());
                break;
            case 8:
                getEncoder().writeTimestamp(properties.getAbsoluteExpiryTime());
                break;
            case 9:
                getEncoder().writeTimestamp(properties.getCreationTime());
                break;
            case 10:
                getEncoder().writeString(properties.getGroupId());
                break;
            case 11:
                getEncoder().writeUnsignedInteger(properties.getGroupSequence());
                break;
            case 12:
                getEncoder().writeString(properties.getReplyToGroupId());
                break;
            default:
                throw new IllegalArgumentException("Unknown Properties value index: " + index);
        }
    }

    private int getElementCount(Properties properties) {
        if (properties.getReplyToGroupId() != null) {
            return 13;
        } else if (properties.getGroupSequence() != null) {
            return 12;
        } else if (properties.getGroupId() != null) {
            return 11;
        } else if (properties.getCreationTime() != null) {
            return 10;
        } else if (properties.getAbsoluteExpiryTime() != null) {
            return 9;
        } else if (properties.getContentEncoding() != null) {
            return 8;
        } else if (properties.getContentType() != null) {
            return 7;
        } else if (properties.getCorrelationId() != null) {
            return 6;
        } else if (properties.getReplyTo() != null) {
            return 5;
        } else if (properties.getSubject() != null) {
            return 4;
        } else if (properties.getTo() != null) {
            return 3;
        } else if (properties.getUserId() != null) {
            return 2;
        } else if (properties.getMessageId() != null) {
            return 1;
        }

        return 0;
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathPropertiesType type = new FastPathPropertiesType(encoder);
        for(Object descriptor : DESCRIPTORS) {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
