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
}
