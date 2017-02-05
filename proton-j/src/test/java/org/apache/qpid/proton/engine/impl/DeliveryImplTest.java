/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.proton.engine.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.test.util.MemoryLeakVerifier;
import org.junit.Test;
import org.mockito.Mockito;

public class DeliveryImplTest
{
    @Test
    public void testDefaultMessageFormat() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);
        assertEquals("Unexpected value", 0L, DeliveryImpl.DEFAULT_MESSAGE_FORMAT);
        assertEquals("Unexpected message format", DeliveryImpl.DEFAULT_MESSAGE_FORMAT, delivery.getMessageFormat());
    }

    @Test
    public void testSetGetMessageFormat() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        // lowest value and default
        int newFormat = 0;
        delivery.setMessageFormat(newFormat);
        assertEquals("Unexpected message format", newFormat, delivery.getMessageFormat());

        newFormat = 123456;
        delivery.setMessageFormat(newFormat);
        assertEquals("Unexpected message format", newFormat, delivery.getMessageFormat());

        // Highest value
        newFormat = (1 << 32) - 1;
        delivery.setMessageFormat(newFormat);
        assertEquals("Unexpected message format", newFormat, delivery.getMessageFormat());
    }

    @Test
    public void testAttachmentsNonNull() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        assertNotNull("Expected attachments to be non-null", delivery.attachments());
    }

    @Test
    public void testAttachmentsReturnsSameRecordOnSuccessiveCalls() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        Record attachments = delivery.attachments();
        Record attachments2 = delivery.attachments();
        assertSame("Expected to get the same attachments", attachments, attachments2);
    }
    @Test
    public void testDelayedSettlementUnLinksProperly()
    {
        int count = 100;

        LinkImpl mockLink = Mockito.mock(LinkImpl.class);
        @SuppressWarnings("deprecation")
        ConnectionImpl mockConnection = new ConnectionImpl();
        Mockito.when(mockLink.getConnectionImpl()).thenReturn(mockConnection);
        
        
        LinkedList<DeliveryImpl> deliveries = new LinkedList<DeliveryImpl>();
        DeliveryImpl last = null;
        for (int i = 0; i < count; i ++) {
            deliveries.add(last = new DeliveryImpl(null, mockLink, last));
            last.setRemoteDeliveryState(Accepted.getInstance());
            mockConnection.workUpdate(last);
            last.setDataLength(i);
            
        }
        for(DeliveryImpl d : deliveries) {
            if (d != deliveries.getFirst()) {
                assertNotNull(d.getDataLength()+" should have work prev", d.getWorkPrev());
            }
            if(d != deliveries.getLast()) {
                assertNotNull(d.getDataLength()+" should have work next", d.getWorkNext());
            }
        }
        Collections.shuffle(deliveries, new Random(1000));
        for(DeliveryImpl d : deliveries) {
            d.settle();
            d.getDataLength();
        }
        for(DeliveryImpl d : deliveries) {
            assertNull("should have no next", d.getLinkNext());
            assertNull("should have no prev", d.getLinkPrevious());
            assertNull("should have no work next", d.getWorkNext());
            assertNull("should have no work prev", d.getWorkPrev());
            if (d != deliveries.getFirst()) {
                assertNotNull(d.getDataLength()+" should have trans work prev", d.getTransportWorkPrev());
            }
            if(d != deliveries.getLast()) {
                assertNotNull(d.getDataLength()+" should have trans work next", d.getTransportWorkNext());
            }
            
        }
        for(DeliveryImpl d : deliveries) {
            d.clearTransportWork();
        }
        for(DeliveryImpl d : deliveries) {
            assertNull("should have no work prev", d.getWorkPrev());
            assertNull("should have no work prev", d.getWorkNext());
        }
        LinkedList<MemoryLeakVerifier> vs = new LinkedList<MemoryLeakVerifier>();
        //HOLD one back to manifest a bug
        last = deliveries.remove(count/2);
        for(DeliveryImpl d : deliveries) {
            vs.add(new MemoryLeakVerifier(d));
        }
        deliveries.clear();
        Mockito.reset(mockLink);
        for(MemoryLeakVerifier v : vs) {
            try{
                v.assertGarbageCollected("DeliverImpl");
            } catch (Throwable t) {
                throw t;
            }
        }
        
    }

}
