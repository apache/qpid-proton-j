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
package org.apache.qpid.proton.codec.transport;

import java.util.Collection;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.AMQPType;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.Decoder;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.FastPathDescribedTypeConstructor;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;

/**
 * Fast TrasnferType encoder
 */
public class FastPathTransferType implements AMQPType<Transfer>, FastPathDescribedTypeConstructor<Transfer> {

    private static final byte DESCRIPTOR_CODE = 0x14;

    private static final Object[] DESCRIPTORS =
    {
        UnsignedLong.valueOf(DESCRIPTOR_CODE), Symbol.valueOf("amqp:transfer:list"),
    };

    private final TransferType transferType;

    public FastPathTransferType(EncoderImpl encoder) {
        this.transferType = new TransferType(encoder);
    }

    public EncoderImpl getEncoder() {
        return transferType.getEncoder();
    }

    public DecoderImpl getDecoder() {
        return transferType.getDecoder();
    }

    @Override
    public Transfer readValue() {
        DecoderImpl decoder = getDecoder();
        byte typeCode = decoder.getBuffer().get();

        @SuppressWarnings("unused")
        int size = 0;
        int count = 0;

        switch (typeCode) {
            case EncodingCodes.LIST0:
                // TODO - Technically invalid however old decoder also allowed this.
                break;
            case EncodingCodes.LIST8:
                size = ((int)decoder.getBuffer().get()) & 0xff;
                count = ((int)decoder.getBuffer().get()) & 0xff;
                break;
            case EncodingCodes.LIST32:
                size = decoder.getBuffer().getInt();
                count = decoder.getBuffer().getInt();
                break;
            default:
                throw new DecodeException("Incorrect type found in Transfer encoding: " + typeCode);
        }

        if (count < 1) {
            throw new DecodeException("The handle field cannot be omitted");
        }

        try {
            return readFields(decoder, count);
        } catch (NullPointerException npe) {
            throw new DecodeException("Unexpected null value - mandatory field not set? (" + npe.getMessage() + ")", npe);
        }
    }

    private final Transfer readFields(DecoderImpl decoder, int count) {
        Transfer transfer = new Transfer();

        for (int index = 0; index < count; ++index) {
            switch (index) {
                case 0:
                    transfer.setHandle(decoder.readUnsignedInteger(null));
                    break;
                case 1:
                    transfer.setDeliveryId(decoder.readUnsignedInteger(null));
                    break;
                case 2:
                    transfer.setDeliveryTag(decoder.readBinary(null));
                    break;
                case 3:
                    transfer.setMessageFormat(decoder.readUnsignedInteger(null));
                    break;
                case 4:
                    transfer.setSettled(decoder.readBoolean(null));
                    break;
                case 5:
                    transfer.setMore(decoder.readBoolean(false));
                    break;
                case 6:
                    UnsignedByte rcvSettleMode = decoder.readUnsignedByte();
                    transfer.setRcvSettleMode(rcvSettleMode == null ? null : ReceiverSettleMode.values()[rcvSettleMode.intValue()]);
                    break;
                case 7:
                    transfer.setState((DeliveryState) decoder.readObject());
                    break;
                case 8:
                    transfer.setResume(decoder.readBoolean(false));
                    break;
                case 9:
                    transfer.setAborted(decoder.readBoolean(false));
                    break;
                case 10:
                    transfer.setBatchable(decoder.readBoolean(false));
                    break;
                default:
                    throw new IllegalStateException("To many entries in Transfer encoding");
            }
        }

        return transfer;
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
    public Class<Transfer> getTypeClass() {
        return Transfer.class;
    }

    @Override
    public TypeEncoding<Transfer> getEncoding(Transfer transfer) {
        return transferType.getEncoding(transfer);
    }

    @Override
    public TypeEncoding<Transfer> getCanonicalEncoding() {
        return transferType.getCanonicalEncoding();
    }

    @Override
    public Collection<? extends TypeEncoding<Transfer>> getAllEncodings() {
        return transferType.getAllEncodings();
    }

    @Override
    public void write(Transfer value) {
        WritableBuffer buffer = getEncoder().getBuffer();
        int count = getElementCount(value);
        byte encodingCode = deduceEncodingCode(value, count);

        buffer.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        buffer.put(EncodingCodes.SMALLULONG);
        buffer.put(DESCRIPTOR_CODE);
        buffer.put(encodingCode);

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

    private void writeElement(Transfer transfer, int index) {
        switch (index) {
            case 0:
                getEncoder().writeUnsignedInteger(transfer.getHandle());
                break;
            case 1:
                getEncoder().writeUnsignedInteger(transfer.getDeliveryId());
                break;
            case 2:
                getEncoder().writeBinary(transfer.getDeliveryTag());
                break;
            case 3:
                getEncoder().writeUnsignedInteger(transfer.getMessageFormat());
                break;
            case 4:
                getEncoder().writeBoolean(transfer.getSettled());
                break;
            case 5:
                getEncoder().writeBoolean(transfer.getMore());
                break;
            case 6:
                ReceiverSettleMode rcvSettleMode = transfer.getRcvSettleMode();
                getEncoder().writeObject(rcvSettleMode == null ? null : rcvSettleMode.getValue());
                break;
            case 7:
                getEncoder().writeObject(transfer.getState());
                break;
            case 8:
                getEncoder().writeBoolean(transfer.getResume());
                break;
            case 9:
                getEncoder().writeBoolean(transfer.getAborted());
                break;
            case 10:
                getEncoder().writeBoolean(transfer.getBatchable());
                break;
            default:
                throw new IllegalArgumentException("Unknown Transfer value index: " + index);
        }
    }

    private byte deduceEncodingCode(Transfer value, int elementCount) {
        if (value.getState() != null) {
            return EncodingCodes.LIST32;
        } else if (value.getDeliveryTag() != null && value.getDeliveryTag().getLength() > 200) {
            return EncodingCodes.LIST32;
        } else {
            return EncodingCodes.LIST8;
        }
    }

    private int getElementCount(Transfer transfer) {
        if (transfer.getBatchable()) {
            return 11;
        } else if (transfer.getAborted()) {
            return 10;
        } else if (transfer.getResume()) {
            return 9;
        } else if (transfer.getState() != null) {
            return 8;
        } else if (transfer.getRcvSettleMode() != null) {
            return 7;
        } else if (transfer.getMore()) {
            return 6;
        } else if (transfer.getSettled() != null) {
            return 5;
        } else if (transfer.getMessageFormat() != null) {
            return 4;
        } else if (transfer.getDeliveryTag() != null) {
            return 3;
        } else if (transfer.getDeliveryId() != null) {
            return 2;
        } else {
            return 1;
        }
    }

    public static void register(Decoder decoder, EncoderImpl encoder) {
        FastPathTransferType type = new FastPathTransferType(encoder);
        for(Object descriptor : DESCRIPTORS)
        {
            decoder.register(descriptor, (FastPathDescribedTypeConstructor<?>) type);
        }
        encoder.register(type);
    }
}
