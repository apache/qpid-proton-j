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
package org.apache.qpid.proton.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.junit.Test;

/**
 * Tests for encode / decode of Transfer types
 */
public class TransferTypeTest extends CodecTestSupport {

    @Test
    public void testEncodeDecodeTransfers() {
        Transfer transfer = new Transfer();
        transfer.setHandle(UnsignedInteger.ONE);
        transfer.setDeliveryTag(new Binary(new byte[] {0, 1}));
        transfer.setMessageFormat(UnsignedInteger.ZERO);
        transfer.setDeliveryId(UnsignedInteger.valueOf(127));
        transfer.setAborted(false);
        transfer.setBatchable(true);
        transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);

        encoder.writeObject(transfer);
        buffer.clear();
        final Transfer outputValue = (Transfer) decoder.readObject();

        assertEquals(transfer.getHandle(), outputValue.getHandle());
        assertEquals(transfer.getMessageFormat(), outputValue.getMessageFormat());
        assertEquals(transfer.getDeliveryTag(), outputValue.getDeliveryTag());
        assertEquals(transfer.getDeliveryId(), outputValue.getDeliveryId());
        assertEquals(transfer.getAborted(), outputValue.getAborted());
        assertEquals(transfer.getBatchable(), outputValue.getBatchable());
        assertEquals(transfer.getRcvSettleMode(), outputValue.getRcvSettleMode());
    }

    @Test
    public void testSkipValue() {
        Transfer transfer = new Transfer();
        transfer.setHandle(UnsignedInteger.ONE);
        transfer.setDeliveryTag(new Binary(new byte[] {0, 1}));
        transfer.setMessageFormat(UnsignedInteger.ZERO);
        transfer.setDeliveryId(UnsignedInteger.valueOf(127));
        transfer.setAborted(false);
        transfer.setBatchable(true);
        transfer.setRcvSettleMode(ReceiverSettleMode.SECOND);

        encoder.writeObject(transfer);

        transfer.setHandle(UnsignedInteger.valueOf(2));

        encoder.writeObject(transfer);

        buffer.clear();

        TypeConstructor<?> type = decoder.readConstructor();
        assertEquals(Transfer.class, type.getTypeClass());
        type.skipValue();

        Transfer result = (Transfer) decoder.readObject();
        assertEquals(UnsignedInteger.valueOf(2), result.getHandle());
    }

    @Test
    public void testDecodeTransfer() throws Exception {
        Object o = doDecodeTransferWithSpecificSizeCountTestImpl(false, 11, 2);
        assertDecodedTransfer(o);

        o = doDecodeTransferWithSpecificSizeCountTestImpl(true, 14, 2);
        assertDecodedTransfer(o);
    }

    private void assertDecodedTransfer(Object o) {
        assertTrue(o instanceof Transfer);
        Transfer decoded = (Transfer) o;

        assertEquals(UnsignedInteger.valueOf(2), decoded.getHandle());
        assertEquals(UnsignedInteger.valueOf(1), decoded.getDeliveryId());
        assertNull(decoded.getDeliveryTag());
        assertNull(decoded.getMessageFormat());
        assertNull(decoded.getSettled());
        assertFalse(decoded.getMore());
        assertNull(decoded.getRcvSettleMode());
        assertNull(decoded.getState());
        assertFalse(decoded.getResume());
        assertFalse(decoded.getAborted());
        assertFalse(decoded.getBatchable());
    }

    @Test
    public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
        try {
            doDecodeTransferWithSpecificSizeCountTestImpl(false, 11 + 1, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 12 cannot be more than remaining bytes 11", de.getMessage());
        }

        try {
            doDecodeTransferWithSpecificSizeCountTestImpl(true, 14 + 1, 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size 15 cannot be more than remaining bytes 14", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeSize() throws Exception {
        try {
            doDecodeTransferWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 2);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List size -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithNegativeCount() throws Exception {
        try {
            doDecodeTransferWithSpecificSizeCountTestImpl(true, 14, UnsignedInteger.MAX_VALUE.intValue());
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count -1 cannot be negative", de.getMessage());
        }
    }

    @Test
    public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeTransferWithSpecificSizeCountTestImpl(false, 2, 3);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 3 must be less than size (2)", de.getMessage());
        }

        try {
            // Deliberately tries not to decode the full list, so the 'remaining' checks dont foul
            doDecodeTransferWithSpecificSizeCountTestImpl(true, 5, 6);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("List element count 6 must be less than size (5)", de.getMessage());
        }
    }

    private Object doDecodeTransferWithSpecificSizeCountTestImpl(final boolean list32, int size, int count) {
        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x14); // transfer ulong descriptor
        if (list32) {
            tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
            tmpBuf.putInt(size); // size
            tmpBuf.putInt(count); // count
        } else {
            tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
            tmpBuf.put((byte) size); // size
            tmpBuf.put((byte) count); // count
        }
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(2); // handle
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(1); // delivery-id

        tmpBuf.flip();

        int oldLimit = tmpBuf.limit();

        decoder.setByteBuffer(tmpBuf);

        final Object decoded;
        try
        {
            decoded = decoder.readObject();
        }
        finally
        {
            assertEquals(oldLimit, tmpBuf.limit());
        }
        assertEquals(oldLimit, tmpBuf.position());

        return decoded;
    }

    @Test
    public void testDecodeOverflowThrows() throws Exception {
        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeTransferWithSpecificSizeCountTestImpl(false, 11 - 5, 2);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }

        try {
            // Deliberately pass size for part list, but count for full list...will try to decode count, overflow size
            doDecodeTransferWithSpecificSizeCountTestImpl(true, 14 - 5, 2);
            fail("Expected exception to be thrown");
        } catch (BufferUnderflowException bue) {
            // Expected
        }
    }

    @Test
    public void testDecodeUnderflowThrows() throws Exception {
        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeTransferWithSpecificSizeCountTestImpl(false, 11, 2 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Transfer incorrectly encoded, left 5 bytes from size indicated 11", de.getMessage());
        }

        try {
            // Deliberately pass size for full list, but count for part list...will try to decode count, underflow size
            doDecodeTransferWithSpecificSizeCountTestImpl(true, 14, 2 - 1);
            fail("Expected exception to be thrown");
        } catch (DecodeException de) {
            assertEquals("Transfer incorrectly encoded, left 5 bytes from size indicated 14", de.getMessage());
        }
    }
}
