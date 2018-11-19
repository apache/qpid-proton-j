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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

public class FlowTest {

    @Test
    public void test() {
        Flow flow = new Flow();

        flow.setNextIncomingId(UnsignedInteger.valueOf(1));
        flow.setIncomingWindow(UnsignedInteger.valueOf(2));
        flow.setNextOutgoingId(UnsignedInteger.valueOf(3));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(4));
        flow.setHandle(UnsignedInteger.valueOf(5));
        flow.setDeliveryCount(UnsignedInteger.valueOf(6));
        flow.setLinkCredit(UnsignedInteger.valueOf(7));
        flow.setAvailable(UnsignedInteger.valueOf(8));
        flow.setDrain(true);
        flow.setEcho(true);
        flow.setProperties(new HashMap<>());

        Flow copyOf = flow.copy();

        assertEquals(flow.getNextIncomingId(), copyOf.getNextIncomingId());
        assertEquals(flow.getIncomingWindow(), copyOf.getIncomingWindow());
        assertEquals(flow.getNextOutgoingId(), copyOf.getNextOutgoingId());
        assertEquals(flow.getOutgoingWindow(), copyOf.getOutgoingWindow());
        assertEquals(flow.getHandle(), copyOf.getHandle());
        assertEquals(flow.getDeliveryCount(), copyOf.getDeliveryCount());
        assertEquals(flow.getLinkCredit(), copyOf.getLinkCredit());
        assertEquals(flow.getAvailable(), copyOf.getAvailable());
        assertEquals(flow.getDrain(), copyOf.getDrain());
        assertEquals(flow.getEcho(), copyOf.getEcho());
        assertEquals(flow.getProperties(), copyOf.getProperties());
    }
}
