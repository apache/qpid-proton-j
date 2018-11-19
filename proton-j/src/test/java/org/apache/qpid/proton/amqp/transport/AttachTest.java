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
package org.apache.qpid.proton.amqp.transport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.junit.Test;

public class AttachTest {

    @Test
    public void testCopy() {
        Attach attach = new Attach();

        attach.setName("test");
        attach.setHandle(UnsignedInteger.ONE);
        attach.setRole(Role.RECEIVER);
        attach.setSndSettleMode(SenderSettleMode.MIXED);
        attach.setRcvSettleMode(ReceiverSettleMode.SECOND);
        attach.setSource(null);
        attach.setTarget(new org.apache.qpid.proton.amqp.messaging.Target());
        attach.setUnsettled(null);
        attach.setIncompleteUnsettled(false);
        attach.setInitialDeliveryCount(UnsignedInteger.valueOf(42));
        attach.setMaxMessageSize(UnsignedLong.valueOf(1024));
        attach.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("anonymous-relay") });
        attach.setDesiredCapabilities(new Symbol[0]);

        final Attach copyOf = attach.copy();

        assertEquals(attach.getName(), copyOf.getName());
        assertArrayEquals(attach.getDesiredCapabilities(), copyOf.getDesiredCapabilities());
        assertEquals(attach.getHandle(), copyOf.getHandle());
        assertEquals(attach.getRole(), copyOf.getRole());
        assertEquals(attach.getSndSettleMode(), copyOf.getSndSettleMode());
        assertEquals(attach.getRcvSettleMode(), copyOf.getRcvSettleMode());
        assertNull(copyOf.getSource());
        assertNotNull(copyOf.getTarget());
        assertEquals(attach.getUnsettled(), copyOf.getUnsettled());
        assertEquals(attach.getIncompleteUnsettled(), copyOf.getIncompleteUnsettled());
        assertEquals(attach.getMaxMessageSize(), copyOf.getMaxMessageSize());
        assertEquals(attach.getInitialDeliveryCount(), copyOf.getInitialDeliveryCount());
        assertArrayEquals(attach.getOfferedCapabilities(), copyOf.getOfferedCapabilities());
    }
}
