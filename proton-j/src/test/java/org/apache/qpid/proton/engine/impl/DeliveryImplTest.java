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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Record;
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
    public void testAvailable() throws Exception
    {
        // Set up a delivery with some data
        byte[] myData = "myData".getBytes(StandardCharsets.UTF_8);

        DeliveryImpl deliveyImpl = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);
        deliveyImpl.setData(myData);
        deliveyImpl.setDataLength(myData.length);

        Delivery delivery = deliveyImpl;

        // Check the full data is available
        assertNotNull("expected the delivery to be present", delivery);
        assertEquals("unexpectd available count", myData.length, delivery.available());

        // Extract some of the data as the receiver link will, check available gets reduced accordingly.
        int partLength = 2;
        int remainderLength = myData.length - partLength;
        assertTrue(partLength < myData.length);

        byte[] myRecievedData1 = new byte[partLength];

        int received = deliveyImpl.recv(myRecievedData1, 0, myRecievedData1.length);
        assertEquals("Unexpected data length received", partLength, received);
        assertEquals("Unexpected data length available", remainderLength, delivery.available());

        // Extract remainder of the data as the receiver link will, check available hits 0.
        byte[] myRecievedData2 = new byte[remainderLength];

        received = deliveyImpl.recv(myRecievedData2, 0, remainderLength);
        assertEquals("Unexpected data length received", remainderLength, received);
        assertEquals("Expected no data to remain available", 0, delivery.available());
    }
}
