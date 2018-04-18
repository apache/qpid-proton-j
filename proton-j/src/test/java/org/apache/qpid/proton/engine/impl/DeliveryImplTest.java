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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.codec.CompositeReadableBuffer;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer.ByteBufferWrapper;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Record;
import org.junit.Test;
import org.mockito.Mockito;

public class DeliveryImplTest
{
    //----- Test for toString ------------------------------------------------//

    @Test
    public void testToStringOnEmptyDelivery() throws Exception
    {
        // Check that no NPE gets thrown when no data in delivery.
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);
        assertNotNull(delivery.toString());
    }

    //----- Tests for message format handling --------------------------------//

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
        deliveyImpl.append(myData);

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

    @Test
    public void testAvailableWhenEmpty() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);
        assertEquals(0, delivery.available());
    }

    //----- Tests for getters of internal Data -------------------------------//

    @Test
    public void testGetDataOnEmptyDelivery() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        assertNotNull(delivery.getData());
        assertFalse(delivery.getData().hasRemaining());
    }

    @Test
    public void testGetDataLengthOnEmptyDelivery() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        assertEquals(0, delivery.getDataLength());
    }

    //----- Tests for Append of Data -----------------------------------------//

    @Test
    public void testAppendArraysToBuffer() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        delivery.append(data1);
        delivery.append(data2);

        assertEquals(data1.length + data2.length, delivery.getDataLength());
        assertNotNull(delivery.getData());
        assertEquals(data1.length + data2.length, delivery.getData().remaining());
    }

    @Test
    public void testAppendBinaryToBuffer() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        Binary binary1 = new Binary(data1);
        Binary binary2 = new Binary(data2);

        delivery.append(binary1);
        delivery.append(binary2);

        assertEquals(data1.length + data2.length, delivery.getDataLength());
        assertNotNull(delivery.getData());
        assertEquals(data1.length + data2.length, delivery.getData().remaining());
    }

    @Test
    public void testAppendBinaryWithOffsetsToBuffer() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        Binary binary1 = new Binary(data1, 1, 2);
        Binary binary2 = new Binary(data2, 0, 4);

        delivery.append(binary1);
        delivery.append(binary2);

        assertEquals(binary1.getLength() + binary2.getLength(), delivery.getDataLength());
        assertNotNull(delivery.getData());
        assertEquals(binary1.getLength() + binary2.getLength(), delivery.getData().remaining());
    }

    //----- Tests for recv all data ------------------------------------------//

    @Test
    public void testRecvAllAsReadableBufferWhenEmpty() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        ReadableBuffer payload = delivery.recv();

        assertNotNull(payload);
        assertEquals(0, payload.remaining());
    }

    @Test
    public void testRecvAllAsReadableBuffer() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        int legnth = data1.length + data2.length;

        Binary binary1 = new Binary(data1);
        Binary binary2 = new Binary(data2);

        delivery.append(binary1);
        delivery.append(binary2);

        ReadableBuffer payload = delivery.recv();
        assertTrue(payload instanceof CompositeReadableBuffer);
        CompositeReadableBuffer composite = (CompositeReadableBuffer) payload;
        assertEquals(2, composite.getArrays().size());

        assertNotNull(payload);
        assertEquals(legnth, payload.remaining());
        assertEquals(0, payload.get(0));
        assertEquals(11, payload.get(11));
    }

    //----- Tests for recv array data ----------------------------------------//

    @Test
    public void testRecvArrayWithEmptyDelivery() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);
        byte[] received = new byte[5];
        assertEquals(0, delivery.recv(received, 0, received.length));
    }

    @Test
    public void testRecvArrayWhenIncomingIsOneArray() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };

        int length = data.length;

        delivery.append(data);

        assertEquals(length, delivery.available());

        byte[] received = new byte[length];

        assertEquals(length, delivery.recv(received, 0, length));

        for (int i = 0; i < length; ++i) {
            assertEquals(received[i], data[i]);
        }

        assertEquals(0, delivery.recv(received, 0, length));
    }

    @Test
    public void testRecvArrayWhenIncomingIsSplitArrays() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        int length = data1.length + data2.length;

        Binary binary1 = new Binary(data1);
        Binary binary2 = new Binary(data2);

        delivery.append(binary1);
        delivery.append(binary2);

        assertEquals(length, delivery.available());

        byte[] received = new byte[length];

        assertEquals(length, delivery.recv(received, 0, length));

        for (int i = 0; i < data1.length; ++i) {
            assertEquals(received[i], data1[i]);
        }

        int offset = data1.length;

        for (int i = 0; i < data2.length; ++i) {
            assertEquals(received[i + offset], data2[i]);
        }

        assertEquals(0, delivery.recv(received, 0, length));
    }

    //----- Tests for recv WritableBuffer ------------------------------------//

    @Test
    public void testRecvWriteableBufferWithEmptyDelivery() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);
        WritableBuffer buffer = WritableBuffer.ByteBufferWrapper.allocate(5);
        assertEquals(0, delivery.recv(buffer));
    }

    @Test
    public void testRecvWritableWhenIncomingIsOneArray() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };

        int length = data.length;

        delivery.append(data);

        assertEquals(length, delivery.available());

        ByteBufferWrapper buffer = WritableBuffer.ByteBufferWrapper.allocate(length);

        assertEquals(length, delivery.recv(buffer));

        ByteBuffer received = buffer.byteBuffer();

        for (int i = 0; i < length; ++i) {
            assertEquals(received.get(i), data[i]);
        }

        assertEquals(0, delivery.recv(WritableBuffer.ByteBufferWrapper.allocate(length)));
    }

    @Test
    public void testRecvWritableWhenIncomingIsSplitArrays() throws Exception
    {
        DeliveryImpl delivery = new DeliveryImpl(null, Mockito.mock(LinkImpl.class), null);

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        int length = data1.length + data2.length;

        delivery.append(data1);
        delivery.append(data2);

        assertEquals(length, delivery.available());

        ByteBufferWrapper buffer = WritableBuffer.ByteBufferWrapper.allocate(length);

        assertEquals(length, delivery.recv(buffer));

        ByteBuffer received = buffer.byteBuffer();

        for (int i = 0; i < data1.length; ++i) {
            assertEquals(received.get(i), data1[i]);
        }

        int offset = data1.length;

        for (int i = 0; i < data2.length; ++i) {
            assertEquals(received.get(i + offset), data2[i]);
        }

        assertEquals(0, delivery.recv(WritableBuffer.ByteBufferWrapper.allocate(length)));
    }

    //----- Test send with byte arrays ---------------------------------------//

    @Test
    public void testSendSingleByteArray() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };

        delivery.send(data, 0, data.length);

        assertEquals(data.length, delivery.pending());
        assertEquals(data.length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data, composite.array());
        assertArrayEquals(data, composite.array());
    }

    @Test
    public void testSendMultipleByteArrays() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        int length = data1.length + data2.length;

        delivery.send(data1, 0, data1.length);
        delivery.send(data2, 0, data2.length);

        assertEquals(length, delivery.pending());
        assertEquals(length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data1, composite.getArrays().get(0));
        assertNotSame(data2, composite.getArrays().get(1));

        assertArrayEquals(data1, composite.getArrays().get(0));
        assertArrayEquals(data2, composite.getArrays().get(1));
    }

    //----- Test send with ReadableBuffer ------------------------------------//

    @Test
    public void testSendSingleReadableBuffer() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };
        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(data);

        delivery.send(buffer);

        assertEquals(data.length, delivery.pending());
        assertEquals(data.length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data, composite.array());
        assertArrayEquals(data, composite.array());
    }

    @Test
    public void testSendSingleReadableBufferWithPartialContent() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] expected = new byte[] { 3, 4, 5 };
        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(data);
        // Now move the position forward so we only send some of the data
        buffer.position(3);

        delivery.send(buffer);

        assertEquals(expected.length, delivery.pending());
        assertEquals(expected.length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data, composite.array());
        assertNotSame(expected, composite.array());
        assertArrayEquals(expected, composite.array());
    }

    @Test
    public void testSendSingleReadableBufferWithOffsetAndPartialContent() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };
        ByteBuffer data = ByteBuffer.wrap(bytes, 0, 5); // Wrap, miss out the last byte from array
        data.position(1); // Now move the position forward
        ByteBuffer dataSlice = data.slice(); // Now slice, causing us to have an array offset at start of array

        byte[] expected = new byte[] { 2, 3, 4 };

        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(dataSlice);
        // Now move the position forward so we only send some of the data
        buffer.position(1);

        assertEquals("Unexpected array offset", 1, buffer.arrayOffset());
        assertEquals("Unexpected remaining", 3, buffer.remaining());

        delivery.send(buffer);

        assertEquals(expected.length, delivery.pending());
        assertEquals(expected.length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(bytes, composite.array());
        assertNotSame(expected, composite.array());
        assertArrayEquals(expected, composite.array());
    }

    @Test
    public void testSendMultipleReadableBuffers() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        ReadableBuffer buffer1 = ReadableBuffer.ByteBufferReader.wrap(data1);
        ReadableBuffer buffer2 = ReadableBuffer.ByteBufferReader.wrap(data2);

        int length = data1.length + data2.length;

        delivery.send(buffer1);
        delivery.send(buffer2);

        assertEquals(length, delivery.pending());
        assertEquals(length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data1, composite.getArrays().get(0));
        assertNotSame(data2, composite.getArrays().get(1));

        assertArrayEquals(data1, composite.getArrays().get(0));
        assertArrayEquals(data2, composite.getArrays().get(1));
    }

    //----- Test send with ReadableBuffer ------------------------------------//

    @Test
    public void testSendNoCopySingleReadableBuffer() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };
        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(data);

        delivery.sendNoCopy(buffer);

        assertEquals(data.length, delivery.pending());
        assertEquals(data.length, delivery.getData().remaining());

        assertSame(buffer, delivery.getData());

        assertSame(data, delivery.getData().array());
        assertArrayEquals(data, delivery.getData().array());
    }

    @Test
    public void testSendNoCopySingleReadableBufferWhenPreviousBufferWasConsumed() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        ReadableBuffer buffer1 = ReadableBuffer.ByteBufferReader.wrap(data1);

        byte[] data2 = new byte[] { 0, 1, 2, 3, 4, 5, 6 };
        ReadableBuffer buffer2 = ReadableBuffer.ByteBufferReader.wrap(data2);

        delivery.sendNoCopy(buffer1);

        assertEquals(data1.length, delivery.pending());
        assertEquals(data1.length, delivery.getData().remaining());

        assertSame(buffer1, delivery.getData());

        assertSame(data1, delivery.getData().array());
        assertArrayEquals(data1, delivery.getData().array());

        delivery.getData().position(delivery.getDataLength());

        delivery.sendNoCopy(buffer2);

        assertEquals(data2.length, delivery.pending());
        assertEquals(data2.length, delivery.getData().remaining());

        assertSame(buffer2, delivery.getData());

        assertSame(data2, delivery.getData().array());
        assertArrayEquals(data2, delivery.getData().array());
    }

    @Test
    public void testSendNoCopyMultipleReadableBuffers() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data1 = new byte[] { 0, 1, 2, 3, 4, 5 };
        byte[] data2 = new byte[] { 6, 7, 8, 9, 10, 11 };

        ReadableBuffer buffer1 = ReadableBuffer.ByteBufferReader.wrap(data1);
        ReadableBuffer buffer2 = ReadableBuffer.ByteBufferReader.wrap(data2);

        int length = data1.length + data2.length;

        delivery.sendNoCopy(buffer1);
        delivery.sendNoCopy(buffer2);

        assertEquals(length, delivery.pending());
        assertEquals(length, delivery.getData().remaining());

        // The Delivery had to copy because it doesn't aggregate buffers only arrays

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data1, composite.getArrays().get(0));
        assertNotSame(data2, composite.getArrays().get(1));

        assertArrayEquals(data1, composite.getArrays().get(0));
        assertArrayEquals(data2, composite.getArrays().get(1));

        byte[] data3 = new byte[] { 12, 13, 14 };
        ReadableBuffer buffer3 = ReadableBuffer.ByteBufferReader.wrap(data3);

        length += data3.length;

        delivery.sendNoCopy(buffer3);

        assertEquals(length, delivery.pending());
        assertEquals(length, delivery.getData().remaining());

        assertSame(composite, delivery.getData());

        assertNotSame(data1, composite.getArrays().get(0));
        assertNotSame(data2, composite.getArrays().get(1));
        assertNotSame(data3, composite.getArrays().get(2));

        assertArrayEquals(data1, composite.getArrays().get(0));
        assertArrayEquals(data2, composite.getArrays().get(1));
        assertArrayEquals(data3, composite.getArrays().get(2));
    }

    //----- Tests for afterSend cleanup --------------------------------------//

    @Test
    public void testAfterSendOnEmptyDelivery() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        ReadableBuffer sendBuffer = delivery.getData();

        delivery.afterSend();

        assertSame(sendBuffer, delivery.getData());
    }

    @Test
    public void testAfterSendPreservesInteralBufferWhenEmpty() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };
        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(data);

        delivery.send(buffer);

        assertEquals(data.length, delivery.pending());
        assertEquals(data.length, delivery.getData().remaining());

        CompositeReadableBuffer composite = (CompositeReadableBuffer) delivery.getData();

        assertNotSame(data, composite.array());
        assertArrayEquals(data, composite.array());

        delivery.getData().position(delivery.getData().limit());
        delivery.afterSend();

        assertSame(composite, delivery.getData());
    }

    @Test
    public void testAfterSendNoCopyClearsExternalReadableBuffer() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };
        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(data);

        delivery.sendNoCopy(buffer);

        ReadableBuffer sendBuffer = delivery.getData();

        assertEquals(data.length, sendBuffer.remaining());
        assertSame(buffer, sendBuffer);

        sendBuffer.position(sendBuffer.limit());

        delivery.afterSend();

        assertNotSame(buffer, delivery.getData());
    }

    @Test
    public void testAfterSendNoCopyPreservesExternalReadableBufferIfNotDrained() throws Exception
    {
        DeliveryImpl delivery = createSenderDelivery();

        byte[] data = new byte[] { 0, 1, 2, 3, 4, 5 };
        ReadableBuffer buffer = ReadableBuffer.ByteBufferReader.wrap(data);

        delivery.sendNoCopy(buffer);

        ReadableBuffer sendBuffer = delivery.getData();

        assertEquals(data.length, sendBuffer.remaining());
        assertSame(buffer, sendBuffer);

        sendBuffer.position(sendBuffer.limit() - 1);

        delivery.afterSend();

        assertSame(buffer, delivery.getData());
    }

    //------------------------------------------------------------------------//

    private DeliveryImpl createSenderDelivery() {
        LinkImpl link = Mockito.mock(SenderImpl.class);
        ConnectionImpl connection = Mockito.mock(ConnectionImpl.class);

        Mockito.when(link.getConnectionImpl()).thenReturn(connection);

        return new DeliveryImpl(null, link, null);
    }
}
