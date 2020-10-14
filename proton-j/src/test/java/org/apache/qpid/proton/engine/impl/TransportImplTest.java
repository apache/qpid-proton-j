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

import static org.apache.qpid.proton.engine.impl.AmqpHeader.HEADER;
import static org.apache.qpid.proton.engine.impl.TransportTestHelper.stringOfLength;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.Close;
import org.apache.qpid.proton.amqp.transport.ConnectionError;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.amqp.transport.Disposition;
import org.apache.qpid.proton.amqp.transport.End;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.apache.qpid.proton.amqp.transport.FrameBody;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.Role;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportException;
import org.apache.qpid.proton.framing.TransportFrame;
import org.apache.qpid.proton.message.Message;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TransportImplTest
{
    private TransportImpl _transport = new TransportImpl();

    private static final int CHANNEL_ID = 1;
    private static final TransportFrame TRANSPORT_FRAME_BEGIN = new TransportFrame(CHANNEL_ID, new Begin(), null);
    private static final TransportFrame TRANSPORT_FRAME_OPEN = new TransportFrame(CHANNEL_ID, new Open(), null);

    private static final int BUFFER_SIZE = 8 * 1024;

    @Rule
    public ExpectedException _expectedException = ExpectedException.none();

    @Test
    public void testInput()
    {
        ByteBuffer buffer = _transport.getInputBuffer();
        buffer.put(HEADER);
        _transport.processInput().checkIsOk();

        assertNotNull(_transport.getInputBuffer());
    }

    @Test
    public void testInitialProcessIsNoop()
    {
        _transport.process();
    }

    @Test
    public void testProcessIsIdempotent()
    {
        _transport.process();
        _transport.process();
    }

    /**
     * Empty input is always allowed by {@link Transport#getInputBuffer()} and
     * {@link Transport#processInput()}, in contrast to the old API.
     *
     * @see TransportImplTest#testEmptyInputBeforeBindUsingOldApi_causesTransportException()
     */
    @Test
    public void testEmptyInput_isAllowed()
    {
        _transport.getInputBuffer();
        _transport.processInput().checkIsOk();
    }

    /**
     * Tests the end-of-stream behaviour specified by {@link Transport#input(byte[], int, int)}.
     */
    @Test
    public void testEmptyInputBeforeBindUsingOldApi_causesTransportException()
    {
        _expectedException.expect(TransportException.class);
        _expectedException.expectMessage("Unexpected EOS when remote connection not closed: connection aborted");
        _transport.input(new byte [0], 0, 0);
    }

    /**
     * TODO it's not clear why empty input is specifically allowed in this case.
     */
    @Test
    public void testEmptyInputWhenRemoteConnectionIsClosedUsingOldApi_isAllowed()
    {
        ConnectionImpl connection = new ConnectionImpl();
        _transport.bind(connection);
        connection.setRemoteState(EndpointState.CLOSED);
        _transport.input(new byte [0], 0, 0);
    }

    @Test
    public void testOutupt()
    {
        {
            // TransportImpl's underlying output spontaneously outputs the AMQP header
            final ByteBuffer outputBuffer = _transport.getOutputBuffer();
            assertEquals(HEADER.length, outputBuffer.remaining());

            byte[] outputBytes = new byte[HEADER.length];
            outputBuffer.get(outputBytes);
            assertArrayEquals(HEADER, outputBytes);

            _transport.outputConsumed();
        }

        {
            final ByteBuffer outputBuffer = _transport.getOutputBuffer();
            assertEquals(0, outputBuffer.remaining());
            _transport.outputConsumed();
        }
    }

    @Test
    public void testOutputBufferIsReadOnly()
    {
        doTestTransportBufferReadability(true, false);
    }

    @Test
    public void testOutputBufferNotReadOnlyWhenConfigured()
    {
        doTestTransportBufferReadability(false, false);
    }

    @Test
    public void testHeadIsReadOnly()
    {
        doTestTransportBufferReadability(true, true);
    }

    @Test
    public void testHeadNotReadOnlyWhenConfigured()
    {
        doTestTransportBufferReadability(false, true);
    }

    private void doTestTransportBufferReadability(boolean readOnly, boolean headOrOutput)
    {
        TransportImpl transport = new TransportImpl();

        // Default should be Read-Only
        if (!readOnly) {
            transport.setUseReadOnlyOutputBuffer(readOnly);
        }

        final ByteBuffer outputBuffer;
        if (headOrOutput) {
            outputBuffer = transport.head();
        } else {
            outputBuffer = transport.getOutputBuffer();
        }

        assertTrue(outputBuffer.hasRemaining());
        if (readOnly) {
            assertTrue(outputBuffer.isReadOnly());
        } else {
            assertFalse(outputBuffer.isReadOnly());
        }

        byte[] outputBytes = new byte[outputBuffer.remaining()];
        outputBuffer.get(outputBytes);

        transport.outputConsumed();

        final ByteBuffer emptyBuffer;
        if (headOrOutput) {
            emptyBuffer = transport.head();
        } else {
            emptyBuffer = transport.getOutputBuffer();
        }

        assertFalse(emptyBuffer.hasRemaining());
        if (readOnly) {
            assertTrue(emptyBuffer.isReadOnly());
        } else {
            assertFalse(emptyBuffer.isReadOnly());
        }
    }

    @Test
    public void testTransportInitiallyHandlesFrames()
    {
        assertTrue(_transport.isHandlingFrames());
    }

    @Test
    public void testBoundTransport_continuesToHandleFrames()
    {
        Connection connection = new ConnectionImpl();

        assertTrue(_transport.isHandlingFrames());

        _transport.bind(connection);

        assertTrue(_transport.isHandlingFrames());

        _transport.handleFrame(TRANSPORT_FRAME_OPEN);

        assertTrue(_transport.isHandlingFrames());
    }

    @Test
    public void testUnboundTransport_stopsHandlingFrames()
    {
        assertTrue(_transport.isHandlingFrames());

        _transport.handleFrame(TRANSPORT_FRAME_OPEN);

        assertFalse(_transport.isHandlingFrames());
    }

    @Test
    public void testHandleFrameWhenNotHandling_throwsIllegalStateException()
    {
        assertTrue(_transport.isHandlingFrames());

        _transport.handleFrame(TRANSPORT_FRAME_OPEN);

        assertFalse(_transport.isHandlingFrames());

        _expectedException.expect(IllegalStateException.class);
        _transport.handleFrame(TRANSPORT_FRAME_BEGIN);
    }

    @Test
    public void testOutputTooBigToBeWrittenInOneGo()
    {
        int smallMaxFrameSize = 512;
        _transport = new TransportImpl(smallMaxFrameSize);

        Connection conn = new ConnectionImpl();
        _transport.bind(conn);

        // Open frame sized in order to produce a frame that will almost fill output buffer
        conn.setHostname(stringOfLength("x", 500));
        conn.open();

        // Close the connection to generate a Close frame which will cause an overflow
        // internally - we'll get the remaining bytes on the next interaction.
        conn.close();

        ByteBuffer buf = _transport.getOutputBuffer();
        assertEquals("Expecting buffer to be full", smallMaxFrameSize, buf.remaining());
        buf.position(buf.limit());
        _transport.outputConsumed();

        buf  = _transport.getOutputBuffer();
        assertTrue("Expecting second buffer to have bytes", buf.remaining() > 0);
        assertTrue("Expecting second buffer to not be full", buf.remaining() < Transport.MIN_MAX_FRAME_SIZE);
    }

    @Test
    public void testAttemptToInitiateSaslAfterProcessingBeginsCausesIllegalStateException()
    {
        _transport.process();

        try
        {
            _transport.sasl();
        }
        catch(IllegalStateException ise)
        {
            //expected, sasl must be initiated before processing begins
        }
    }

    @Test
    public void testChannelMaxDefault() throws Exception
    {
        Transport transport = Proton.transport();

        assertEquals("Unesxpected value for channel-max", 65535, transport.getChannelMax());
    }

    @Test
    public void testSetGetChannelMax() throws Exception
    {
        Transport transport = Proton.transport();

        int channelMax = 456;
        transport.setChannelMax(channelMax);
        assertEquals("Unesxpected value for channel-max", channelMax, transport.getChannelMax());
    }

    @Test
    public void testSetChannelMaxOutsideLegalUshortRangeThrowsIAE() throws Exception
    {
        Transport transport = Proton.transport();

        try {
            transport.setChannelMax( 1 << 16);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae ){
            // Expected
        }

        try {
            transport.setChannelMax(-1);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae ){
            // Expected
        }
    }

    private class MockTransportImpl extends TransportImpl
    {
        public MockTransportImpl() {
            super();
        }

        public MockTransportImpl(int maxFrameSize) {
            super(maxFrameSize);
        }

        LinkedList<FrameBody> writes = new LinkedList<FrameBody>();

        @Override
        protected void writeFrame(int channel, FrameBody frameBody,
                                  ReadableBuffer payload, Runnable onPayloadTooLarge) {
            super.writeFrame(channel, frameBody, payload, onPayloadTooLarge);
            writes.addLast(frameBody != null ? frameBody.copy() : null);
        }
    }

    @Test
    public void testTickRemoteTimeout()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        int timeout = 4000;
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(4000));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(0);
        assertEquals("Expected to be returned a deadline of 2000",  2000, deadline);  // deadline = 4000 / 2

        deadline = transport.tick(1000);    // Wait for less than the deadline with no data - get the same value
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  2000, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 0, transport.writes.size());

        deadline = transport.tick(timeout/2); // Wait for the deadline - next deadline should be (4000/2)*2
        assertEquals("When the deadline has been reached expected a new deadline to be returned 4000",  4000, deadline);
        assertEquals("tick() should have written data", 1, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(0));

        transport.writeFrame(CHANNEL_ID, new Begin(), null, null);
        while(transport.pending() > 0) transport.pop(transport.head().remaining());
        int framesWrittenBeforeTick = transport.writes.size();
        deadline = transport.tick(3000);
        assertEquals("Writing data resets the deadline",  5000, deadline);
        assertEquals("When the deadline is reset tick() shouldn't write an empty frame", 0, transport.writes.size() - framesWrittenBeforeTick);

        transport.writeFrame(CHANNEL_ID, new Attach(), null, null);
        assertTrue(transport.pending() > 0);
        framesWrittenBeforeTick = transport.writes.size();
        deadline = transport.tick(4000);
        assertEquals("Having pending data does not reset the deadline",  5000, deadline);
        assertEquals("Having pending data prevents tick() from sending an empty frame", 0, transport.writes.size() - framesWrittenBeforeTick);
    }

    @Test
    public void testTickLocalTimeout()
    {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setIdleTimeout(4000);
        Connection connection = Proton.connection();
        transport.bind(connection);

        transport.handleFrame(TRANSPORT_FRAME_OPEN);
        connection.open();
        pumpMockTransport(transport);

        long deadline = transport.tick(0);
        assertEquals("Expected to be returned a deadline of 4000",  4000, deadline);

        int framesWrittenBeforeTick = transport.writes.size();
        deadline = transport.tick(1000);    // Wait for less than the deadline with no data - get the same value
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  4000, deadline);
        assertEquals("Reading data should never result in a frame being written", 0, transport.writes.size() - framesWrittenBeforeTick);

        // Protocol header + empty frame
        ByteBuffer data = ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00});
        processInput(transport, data);
        framesWrittenBeforeTick = transport.writes.size();
        deadline = transport.tick(2000);
        assertEquals("Reading data data resets the deadline",  6000, deadline);
        assertEquals("Reading data should never result in a frame being written", 0, transport.writes.size() - framesWrittenBeforeTick);
        assertEquals("Reading data before the deadline should keep the connection open", EndpointState.ACTIVE, connection.getLocalState());

        framesWrittenBeforeTick = transport.writes.size();
        deadline = transport.tick(7000);
        assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
    }

    /*
     * No frames should be written until the Connection object is
     * opened, at which point the Open, and Begin frames should
     * be pipelined together.
     */
    @Test
    public void testOpenSessionBeforeOpenConnection()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        Session session = connection.session();
        session.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
    }

    /*
     * No frames should be written until the Connection object is
     * opened, at which point the Open, Begin, and Attach frames
     * should be pipelined together.
     */
    @Test
    public void testOpenReceiverBeforeOpenConnection()
    {
        doOpenLinkBeforeOpenConnectionTestImpl(true);
    }

    /**
     * No frames should be written until the Connection object is
     * opened, at which point the Open, Begin, and Attach frames
     * should be pipelined together.
     */
    @Test
    public void testOpenSenderBeforeOpenConnection()
    {
        doOpenLinkBeforeOpenConnectionTestImpl(false);
    }

    void doOpenLinkBeforeOpenConnectionTestImpl(boolean receiverLink)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        Session session = connection.session();
        session.open();

        Link link = null;
        if(receiverLink)
        {
            link = session.receiver("myReceiver");
        }
        else
        {
            link = session.sender("mySender");
        }
        link.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        // Now open the connection, expect the Open, Begin, and Attach frames
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
    }

    /*
     * No attach frame should be written before the Session begin is sent.
     */
    @Test
    public void testOpenReceiverBeforeOpenSession()
    {
        doOpenLinkBeforeOpenSessionTestImpl(true);
    }

    /*
     * No attach frame should be written before the Session begin is sent.
     */
    @Test
    public void testOpenSenderBeforeOpenSession()
    {
        doOpenLinkBeforeOpenSessionTestImpl(false);
    }

    void doOpenLinkBeforeOpenSessionTestImpl(boolean receiverLink)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Open the connection
        connection.open();

        // Create but don't open the session
        Session session = connection.session();

        // Open the link
        Link link = null;
        if(receiverLink)
        {
            link = session.receiver("myReceiver");
        }
        else
        {
            link = session.sender("mySender");
        }
        link.open();

        pumpMockTransport(transport);

        // Expect only an Open frame, no attach should be sent as the session isn't open
        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);

        // Now open the session, expect the Begin
        session.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        // Note: an Attach wasn't sent because link is no longer 'modified' after earlier pump. It
        // could easily be argued it should, given how the engine generally handles things. Seems
        // unlikely to be of much real world concern.
        //assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
    }

    /*
     * Verify that no Attach frame is emitted by the Transport should a Receiver
     * be opened after the session End frame was sent.
     */
    @Test
    public void testReceiverAttachAfterEndSent()
    {
        doLinkAttachAfterEndSentTestImpl(true);
    }

    /*
     * Verify that no Attach frame is emitted by the Transport should a Sender
     * be opened after the session End frame was sent.
     */
    @Test
    public void testSenderAttachAfterEndSent()
    {
        doLinkAttachAfterEndSentTestImpl(false);
    }

    void doLinkAttachAfterEndSentTestImpl(boolean receiverLink)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        Link link = null;
        if(receiverLink)
        {
            link = session.receiver("myReceiver");
        }
        else
        {
            link = session.sender("mySender");
        }

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);

        // Send the necessary responses to open/begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        // Cause a End frame to be sent
        session.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof End);

        // Open the link and verify the transport doesn't
        // send any Attach frame, as an End frame was sent already.
        link.open();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
    }

    /*
     * Verify that no Attach frame is emitted by the Transport should a Receiver
     * be closed after the session End frame was sent.
     */
    @Test
    public void testReceiverCloseAfterEndSent()
    {
        doLinkDetachAfterEndSentTestImpl(true);
    }

    /*
     * Verify that no Attach frame is emitted by the Transport should a Sender
     * be closed after the session End frame was sent.
     */
    @Test
    public void testSenderCloseAfterEndSent()
    {
        doLinkDetachAfterEndSentTestImpl(false);
    }

    void doLinkDetachAfterEndSentTestImpl(boolean receiverLink)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        Link link = null;
        if(receiverLink)
        {
            link = session.receiver("myReceiver");
        }
        else
        {
            link = session.sender("mySender");
        }
        link.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Cause an End frame to be sent
        session.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof End);

        // Close the link and verify the transport doesn't
        // send any Detach frame, as an End frame was sent already.
        link.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
    }

    /*
     * No frames should be written until the Connection object is
     * opened, at which point the Open and Begin frames should
     * be pipelined together.
     */
    @Test
    public void testReceiverFlowBeforeOpenConnection()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        Session session = connection.session();
        session.open();

        Receiver reciever = session.receiver("myReceiver");
        reciever.flow(5);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        // Now open the connection, expect the Open and Begin frames but
        // nothing else as we haven't opened the receiver itself yet.
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
    }

    @Test
    public void testSenderSendBeforeOpenConnection()
    {
        MockTransportImpl transport = new MockTransportImpl();

        Connection connection = Proton.connection();
        transport.bind(connection);

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        sendMessage(sender, "tag1", "content1");

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        // Now open the connection, expect the Open and Begin and Attach frames but
        // nothing else as we the sender wont have credit yet.
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach then give sender credit
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setLinkCredit(UnsignedInteger.valueOf(10));

        transport.handleFrame(new TransportFrame(0, flow, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Now pump the transport again and expect a transfer for the message
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Transfer);
    }

    @Test
    public void testEmitFlowEventOnSend()
    {
        doEmitFlowOnSendTestImpl(true);
    }

    public void testSupressFlowEventOnSend()
    {
        doEmitFlowOnSendTestImpl(false);
    }

    void doEmitFlowOnSendTestImpl(boolean emitFlowEventOnSend)
    {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(emitFlowEventOnSend);

        Connection connection = Proton.connection();
        transport.bind(connection);

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        sendMessage(sender, "tag1", "content1");

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        assertEvents(collector, Event.Type.CONNECTION_INIT, Event.Type.SESSION_INIT, Event.Type.SESSION_LOCAL_OPEN,
                                Event.Type.TRANSPORT, Event.Type.LINK_INIT, Event.Type.LINK_LOCAL_OPEN, Event.Type.TRANSPORT);

        // Now open the connection, expect the Open and Begin frames but
        // nothing else as we haven't opened the receiver itself yet.
        connection.open();

        pumpMockTransport(transport);

        assertEvents(collector, Event.Type.CONNECTION_LOCAL_OPEN, Event.Type.TRANSPORT);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach then give sender credit
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setLinkCredit(UnsignedInteger.valueOf(10));

        transport.handleFrame(new TransportFrame(0, flow, null));

        assertEvents(collector, Event.Type.CONNECTION_REMOTE_OPEN, Event.Type.SESSION_REMOTE_OPEN,
                                Event.Type.LINK_REMOTE_OPEN, Event.Type.LINK_FLOW);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Now pump the transport again and expect a transfer for the message
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Transfer);

        // Verify that we did, or did not, emit a flow event
        if(emitFlowEventOnSend)
        {
            assertEvents(collector, Event.Type.LINK_FLOW);
        }
        else
        {
            assertNoEvents(collector);
        }
    }

    /**
     * Verify that no Begin frame is emitted by the Transport should a Session open
     * after the Close frame was sent.
     */
    @Test
    public void testSessionBeginAfterCloseSent()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);

        // Send the necessary response to Open
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());

        // Cause a Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Close);

        // Open the session and verify the transport doesn't
        // send any Begin frame, as a Close frame was sent already.
        session.open();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
    }

    /**
     * Verify that no End frame is emitted by the Transport should a Session close
     * after the Close frame was sent.
     */
    @Test
    public void testSessionEndAfterCloseSent()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);

        // Send the necessary responses to open/begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        // Cause a Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Close);

        // Close the session and verify the transport doesn't
        // send any End frame, as a Close frame was sent already.
        session.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
    }

    @Test
    public void testEmittedSessionIncomingWindow()
    {
        doSessionIncomingWindowTestImpl(false, false);
        doSessionIncomingWindowTestImpl(true, false);
        doSessionIncomingWindowTestImpl(false, true);
        doSessionIncomingWindowTestImpl(true, true);
    }

    private void doSessionIncomingWindowTestImpl(boolean setFrameSize, boolean setSessionCapacity) {
        MockTransportImpl transport;
        if(setFrameSize) {
            transport = new MockTransportImpl(5*1024);
        } else {
            transport = new MockTransportImpl();
        }

        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        int sessionCapacity = 0;
        if(setSessionCapacity) {
            sessionCapacity = 100*1024;
            session.setIncomingCapacity(sessionCapacity);
        }

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        // Provide an Open response
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        // Open session and verify emitted incoming window
        session.open();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        Begin sentBegin = (Begin) transport.writes.get(1);

        assertEquals("Unexpected session capacity", sessionCapacity, session.getIncomingCapacity());

        int expectedWindowSize = 2147483647;
        if(setSessionCapacity && setFrameSize) {
            expectedWindowSize = (100*1024) / (5*1024); // capacity / frameSize
        }

        assertEquals("Unexpected session window", UnsignedInteger.valueOf(expectedWindowSize), sentBegin.getIncomingWindow());

        // Open receiver
        String linkName = "myReceiver";
        Receiver receiver = session.receiver(linkName);
        receiver.open();

        pumpMockTransport(transport);

        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Provide an begin+attach response
        Begin beginResponse = new Begin();
        beginResponse.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        beginResponse.setNextOutgoingId(UnsignedInteger.ONE);
        beginResponse.setIncomingWindow(UnsignedInteger.valueOf(1024));
        beginResponse.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, beginResponse, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.SENDER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Flow some credit, verify emitted incoming window remains the same
        receiver.flow(1);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Flow);
        Flow sentFlow = (Flow) transport.writes.get(3);

        assertEquals("Unexpected session window", UnsignedInteger.valueOf(expectedWindowSize), sentFlow.getIncomingWindow());

        // Provide a transfer, don't consume it, flow more credit, verify the emitted
        // incoming window (should reduce 1 if capacity and frame size set)
        String deliveryTag = "tag1";
        String messageContent = "content1";
        handleTransfer(transport, 1, deliveryTag, messageContent);

        assertTrue("Unexpected session byte count", session.getIncomingBytes() > 0);

        receiver.flow(1);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Flow);
        sentFlow = (Flow) transport.writes.get(4);

        if(setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize -1;
        }
        assertEquals("Unexpected session window", UnsignedInteger.valueOf(expectedWindowSize), sentFlow.getIncomingWindow());

        // Consume the transfer then flow more credit, verify the emitted
        // incoming window (should increase 1 if capacity and frame size set)
        verifyDelivery(receiver, deliveryTag, messageContent);
        assertEquals("Unexpected session byte count", 0, session.getIncomingBytes());

        receiver.flow(1);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(5) instanceof Flow);
        sentFlow = (Flow) transport.writes.get(5);

        if(setSessionCapacity && setFrameSize) {
            expectedWindowSize = expectedWindowSize +1;
        }
        assertEquals("Unexpected session window", UnsignedInteger.valueOf(expectedWindowSize), sentFlow.getIncomingWindow());
    }

    /**
     * Verify that no Attach frame is emitted by the Transport should a Receiver
     * be opened after the Close frame was sent.
     */
    @Test
    public void testReceiverAttachAfterCloseSent()
    {
        doLinkAttachAfterCloseSentTestImpl(true);
    }

    /**
     * Verify that no Attach frame is emitted by the Transport should a Sender
     * be opened after the Close frame was sent.
     */
    @Test
    public void testSenderAttachAfterCloseSent()
    {
        doLinkAttachAfterCloseSentTestImpl(false);
    }

    void doLinkAttachAfterCloseSentTestImpl(boolean receiverLink)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        Link link = null;
        if(receiverLink)
        {
            link = session.receiver("myReceiver");
        }
        else
        {
            link = session.sender("mySender");
        }

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);

        // Send the necessary responses to open/begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        // Cause a Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Close);

        // Open the link and verify the transport doesn't
        // send any Attach frame, as a Close frame was sent already.
        link.open();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
    }

    /**
     * Verify that no Flow frame is emitted by the Transport should a Receiver
     * have credit added after the Close frame was sent.
     */
    @Test
    public void testReceiverFlowAfterCloseSent()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "myReceiver";
        Receiver receiver = session.receiver(linkName);
        receiver.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Cause the Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Close);

        // Grant new credit for the Receiver and verify the transport doesn't
        // send any Flow frame, as a Close frame was sent already.
        receiver.flow(1);
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
    }

    /**
     * Verify that no Flow frame is emitted by the Transport should a Receiver
     * have pending drain when a detach is sent for that receiver.
     */
    @Test
    public void testNoReceiverFlowAfterDetachSentWhileDraining()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "myReceiver";
        Receiver receiver = session.receiver(linkName);
        receiver.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Start a drain for the Receiver and verify the transport doesn't
        // send any Flow frame, due to the detach being initiated.
        receiver.drain(10);
        pumpMockTransport(transport);

        // Cause the Detach frame to be sent
        receiver.detach();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Detach);
    }

    /**
     * Verify that no Flow frame is emitted by the Transport should a Sender
     * have credit drained added after the Close frame was sent.
     */
    @Test
    public void testSenderFlowAfterCloseSent()
    {
        MockTransportImpl transport = new MockTransportImpl();

        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        assertFalse("Should not be in drain yet", sender.getDrain());

        // Send the necessary responses to open/begin/attach then give sender credit and drain
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        int credit = 10;
        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setDrain(true);
        flow.setLinkCredit(UnsignedInteger.valueOf(credit));

        transport.handleFrame(new TransportFrame(0, flow, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Should not be in drain", sender.getDrain());
        assertEquals("Should have credit", credit, sender.getCredit());

        // Cause the Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Close);

        // Drain the credit and verify the transport doesn't
        // send any Flow frame, as a Close frame was sent already.
        int drained = sender.drained();
        assertEquals("Should have drained all credit", credit, drained);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
    }

    /**
     * Verify that no Disposition frame is emitted by the Transport should a Delivery
     * have disposition applied after the Close frame was sent.
     */
    @Test
    public void testDispositionAfterCloseSent()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "myReceiver";
        Receiver receiver = session.receiver(linkName);
        receiver.flow(5);
        receiver.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Flow);

        Delivery delivery = receiver.current();
        assertNull("Should not yet have a delivery", delivery);

        // Send the necessary responses to open/begin/attach as well as a transfer
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.SENDER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        String deliveryTag = "tag1";
        String messageContent = "content1";
        handleTransfer(transport, 1, deliveryTag, messageContent);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        delivery = verifyDelivery(receiver, deliveryTag, messageContent);
        assertNotNull("Should now have a delivery", delivery);

        // Cause the Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Close);

        delivery.disposition(Released.getInstance());
        delivery.settle();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
    }

    /**
     * Verify that no Transfer frame is emitted by the Transport should a Delivery
     * be sendable after the Close frame was sent.
     */
    @Test
    public void testTransferAfterCloseSent()
    {
        MockTransportImpl transport = new MockTransportImpl();

        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach then give sender credit
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setLinkCredit(UnsignedInteger.valueOf(10));

        transport.handleFrame(new TransportFrame(0, flow, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Cause the Close frame to be sent
        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Close);

        // Send a new message and verify the transport doesn't
        // send any Transfer frame, as a Close frame was sent already.
        sendMessage(sender, "tag1", "content1");
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
    }

    private void assertNoEvents(Collector collector)
    {
        assertEvents(collector);
    }

    private void assertEvents(Collector collector, Event.Type... expectedEventTypes)
    {

        if(expectedEventTypes.length == 0)
        {
            assertNull("Expected no events, but at least one was present: " + collector.peek(), collector.peek());
        }
        else
        {
            ArrayList<Event.Type> eventTypesList = new ArrayList<Event.Type>();
            Event event = null;
            while ((event = collector.peek()) != null) {
                eventTypesList.add(event.getType());
                collector.pop();
            }

            assertArrayEquals("Unexpected event types: " + eventTypesList, expectedEventTypes, eventTypesList.toArray(new Event.Type[0]));
        }
    }

    private void pumpMockTransport(MockTransportImpl transport)
    {
        while(transport.pending() > 0)
        {
            transport.pop(transport.head().remaining());
        }
    }

    private String getFrameTypesWritten(MockTransportImpl transport)
    {
        String result = "";
        for(FrameBody f : transport.writes) {
            result += f.getClass().getSimpleName();
            result += ",";
        }

        if(result.isEmpty()) {
            return "no-frames-written";
        } else {
            return result;
        }
    }

    private Delivery sendMessage(Sender sender, String deliveryTag, String messageContent)
    {
        byte[] tag = deliveryTag.getBytes(StandardCharsets.UTF_8);

        Message m = Message.Factory.create();
        m.setBody(new AmqpValue(messageContent));

        byte[] encoded = new byte[BUFFER_SIZE];
        int len = m.encode(encoded, 0, BUFFER_SIZE);

        assertTrue("given array was too small", len < BUFFER_SIZE);

        Delivery delivery = sender.delivery(tag);

        int sent = sender.send(encoded, 0, len);

        assertEquals("sender unable to send all data at once as assumed for simplicity", len, sent);

        boolean senderAdvanced = sender.advance();
        assertTrue("sender has not advanced", senderAdvanced);

        return delivery;
    }

    private void handleTransfer(TransportImpl transport, int deliveryNumber, String deliveryTag, String messageContent)
    {
        byte[] tag = deliveryTag.getBytes(StandardCharsets.UTF_8);

        Message m = Message.Factory.create();
        m.setBody(new AmqpValue(messageContent));

        byte[] encoded = new byte[BUFFER_SIZE];
        int len = m.encode(encoded, 0, BUFFER_SIZE);

        assertTrue("given array was too small", len < BUFFER_SIZE);

        Transfer transfer = new Transfer();
        transfer.setDeliveryId(UnsignedInteger.valueOf(deliveryNumber));
        transfer.setHandle(UnsignedInteger.ZERO);
        transfer.setDeliveryTag(new Binary(tag));
        transfer.setMessageFormat(UnsignedInteger.valueOf(DeliveryImpl.DEFAULT_MESSAGE_FORMAT));

        transport.handleFrame(new TransportFrame(0, transfer, new Binary(encoded, 0, len)));
    }

    private Delivery verifyDelivery(Receiver receiver, String deliveryTag, String messageContent)
    {
        Delivery delivery = receiver.current();

        assertTrue(Arrays.equals(deliveryTag.getBytes(StandardCharsets.UTF_8), delivery.getTag()));

        assertNull(delivery.getLocalState());
        assertNull(delivery.getRemoteState());

        assertFalse(delivery.isPartial());
        assertTrue(delivery.isReadable());

        byte[] received = new byte[BUFFER_SIZE];
        int len = receiver.recv(received, 0, BUFFER_SIZE);

        assertTrue("given array was too small", len < BUFFER_SIZE);

        Message m = Proton.message();
        m.decode(received, 0, len);

        Object messageBody = ((AmqpValue)m.getBody()).getValue();
        assertEquals("Unexpected message content", messageContent, messageBody);

        boolean receiverAdvanced = receiver.advance();
        assertTrue("receiver has not advanced", receiverAdvanced);

        return delivery;
    }

    private Delivery verifyDeliveryRawPayload(Receiver receiver, String deliveryTag, byte[] payload)
    {
        Delivery delivery = receiver.current();

        assertTrue(Arrays.equals(deliveryTag.getBytes(StandardCharsets.UTF_8), delivery.getTag()));

        assertFalse(delivery.isPartial());
        assertTrue(delivery.isReadable());

        byte[] received = new byte[delivery.pending()];
        int len = receiver.recv(received, 0, BUFFER_SIZE);

        assertEquals("unexpected length", len, received.length);

        assertArrayEquals("Received delivery payload not as expected", payload, received);

        boolean receiverAdvanced = receiver.advance();
        assertTrue("receiver has not advanced", receiverAdvanced);

        return delivery;
    }

    /**
     * Verify that the {@link TransportInternal#addTransportLayer(TransportLayer)} has the desired
     * effect by observing the wrapping effect on related transport input and output methods.
     */
    @Test
    public void testAddAdditionalTransportLayer()
    {
        Integer capacityOverride = 1957;
        Integer pendingOverride = 2846;

        MockTransportImpl transport = new MockTransportImpl();

        TransportWrapper mockWrapper = Mockito.mock(TransportWrapper.class);

        Mockito.when(mockWrapper.capacity()).thenReturn(capacityOverride);
        Mockito.when(mockWrapper.pending()).thenReturn(pendingOverride);

        TransportLayer mockLayer = Mockito.mock(TransportLayer.class);
        Mockito.when(mockLayer.wrap(Mockito.any(TransportInput.class), Mockito.any(TransportOutput.class))).thenReturn(mockWrapper);

        transport.addTransportLayer(mockLayer);

        assertEquals("Unexepcted value, layer override not effective", capacityOverride.intValue(), transport.capacity());
        assertEquals("Unexepcted value, layer override not effective", pendingOverride.intValue(), transport.pending());
    }

    @Test
    public void testAddAdditionalTransportLayerThrowsISEIfProcessingStarted()
    {
        MockTransportImpl transport = new MockTransportImpl();
        TransportLayer mockLayer = Mockito.mock(TransportLayer.class);

        transport.process();

        try
        {
            transport.addTransportLayer(mockLayer);
            fail("Expected exception to be thrown due to processing having started");
        }
        catch (IllegalStateException ise)
        {
            // expected
        }
    }

    @Test
    public void testEndpointOpenAndCloseAreIdempotent()
    {
        MockTransportImpl transport = new MockTransportImpl();

        Connection connection = Proton.connection();
        transport.bind(connection);

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        connection.open();
        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        pumpMockTransport(transport);

        assertEvents(collector, Event.Type.CONNECTION_INIT, Event.Type.CONNECTION_LOCAL_OPEN, Event.Type.TRANSPORT,
                                Event.Type.SESSION_INIT, Event.Type.SESSION_LOCAL_OPEN,
                                Event.Type.TRANSPORT, Event.Type.LINK_INIT, Event.Type.LINK_LOCAL_OPEN, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        connection.open();
        session.open();
        sender.open();

        assertNoEvents(collector);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach then give sender credit
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        assertEvents(collector, Event.Type.CONNECTION_REMOTE_OPEN, Event.Type.SESSION_REMOTE_OPEN,
                                Event.Type.LINK_REMOTE_OPEN);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Now close the link and expect one event
        sender.close();
        sender.close();

        assertEvents(collector, Event.Type.LINK_LOCAL_CLOSE, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        sender.close();

        assertNoEvents(collector);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Detach);
    }

    @Test
    public void testInitialRemoteMaxFrameSizeOverride()
    {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setInitialRemoteMaxFrameSize(768);

        assertEquals("Unexpected value : " + getFrameTypesWritten(transport), 768, transport.getRemoteMaxFrameSize());

        Connection connection = Proton.connection();
        transport.bind(connection);
        connection.open();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);

        try
        {
            transport.setInitialRemoteMaxFrameSize(12345);
            fail("expected an exception");
        }
        catch (IllegalStateException ise )
        {
            //expected
        }

        // Send the necessary responses to open
        Open open = new Open();
        open.setMaxFrameSize(UnsignedInteger.valueOf(4567));
        transport.handleFrame(new TransportFrame(0, open, null));

        assertEquals("Unexpected value : " + getFrameTypesWritten(transport), 4567, transport.getRemoteMaxFrameSize());
    }

    @Test
    public void testTickWithZeroIdleTimeoutsGivesZeroDeadline()
    {
        doTickWithNoIdleTimeoutGivesZeroDeadlineTestImpl(true);
    }

    @Test
    public void testTickWithNullIdleTimeoutsGivesZeroDeadline()
    {
        doTickWithNoIdleTimeoutGivesZeroDeadlineTestImpl(false);
    }

    private void doTickWithNoIdleTimeoutGivesZeroDeadlineTestImpl(boolean useZero) {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();
        while(transport.pending() > 0) {
            transport.pop(transport.head().remaining());
        }

        assertEquals("should have written data", 1, transport.writes.size());
        FrameBody sentOpenFrame = transport.writes.get(0);
        assertNotNull("should have written a non-empty frame", sentOpenFrame);
        assertTrue("should have written an open frame", sentOpenFrame instanceof Open);
        assertNull("should not have had an idletimeout value", ((Open)sentOpenFrame).getIdleTimeOut());

        // Handle the peer transmitting their open with null/zero timeout.
        Open open = new Open();
        if(useZero) {
            open.setIdleTimeOut(UnsignedInteger.ZERO);
        } else {
            open.setIdleTimeOut(null);
        }
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(0);
        assertEquals("Unexpected deadline returned", 0, deadline);

        deadline = transport.tick(10);
        assertEquals("Unexpected deadline returned", 0, deadline);
    }

    @Test
    public void testTickWithLocalTimeout()
    {
        // all-positive
        doTickWithLocalTimeoutTestImpl(4000, 10000, 14000, 18000, 22000);

        // all-negative
        doTickWithLocalTimeoutTestImpl(2000, -100000, -98000, -96000, -94000);

        // negative to positive missing 0
        doTickWithLocalTimeoutTestImpl(500, -950, -450, 50, 550);

        // negative to positive striking 0
        doTickWithLocalTimeoutTestImpl(3000, -6000, -3000, 1, 3001);
    }

    private void doTickWithLocalTimeoutTestImpl(int localTimeout, long tick1, long expectedDeadline1, long expectedDeadline2, long expectedDeadline3)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Set our local idleTimeout
        transport.setIdleTimeout(localTimeout);

        connection.open();
        pumpMockTransport(transport);

        assertEquals("should have written data", 1, transport.writes.size());
        Object sentOpenFrame = transport.writes.get(0);
        assertNotNull("should have written a non-empty frame", sentOpenFrame);
        assertTrue("should have written an open frame", sentOpenFrame instanceof Open);
        assertEquals("should have had an idletimeout value half our actual timeout", UnsignedInteger.valueOf(localTimeout / 2), ((Open)sentOpenFrame).getIdleTimeOut());

        // Receive Protocol header
        processInput(transport, ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00}));

        // Handle the peer transmitting their open, without timeout.
        Open open = new Open();
        open.setIdleTimeOut(null);
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(tick1);
        assertEquals("Unexpected deadline returned", expectedDeadline1, deadline);

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue (interimTick < expectedDeadline1);
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  expectedDeadline1, transport.tick(interimTick));
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        // Receive Empty frame to satisfy local deadline
        processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

        deadline = transport.tick(expectedDeadline1);
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", expectedDeadline2, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        pumpMockTransport(transport);

        // Receive Empty frame to satisfy local deadline
        processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

        deadline = transport.tick(expectedDeadline2);
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", expectedDeadline3, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        pumpMockTransport(transport);

        assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
        transport.tick(expectedDeadline3); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
        assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
        assertEquals("tick() should have written data", 2, transport.writes.size());
        assertNotNull("should have written a non-empty frame", transport.writes.get(1));
        assertTrue("should have written a close frame", transport.writes.get(1) instanceof Close);
    }

    @Test
    public void testTickWithRemoteTimeout()
    {
        // all-positive
        doTickWithRemoteTimeoutTestImpl(4000, 10000, 14000, 18000, 22000);

        // all-negative
        doTickWithRemoteTimeoutTestImpl(2000, -100000, -98000, -96000, -94000);

        // negative to positive missing 0
        doTickWithRemoteTimeoutTestImpl(500, -950, -450, 50, 550);

        // negative to positive striking 0
        doTickWithRemoteTimeoutTestImpl(3000, -6000, -3000, 1, 3001);
    }

    private void doTickWithRemoteTimeoutTestImpl(int remoteTimeoutHalf, long tick1, long expectedDeadline1, long expectedDeadline2, long expectedDeadline3)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();
        pumpMockTransport(transport);

        assertEquals("should have written data", 1, transport.writes.size());
        Object sentOpenFrame = transport.writes.get(0);
        assertNotNull("should have written a non-empty frame", sentOpenFrame);
        assertTrue("should have written an open frame", sentOpenFrame instanceof Open);
        assertNull("should not have had an idletimeout value", ((Open)sentOpenFrame).getIdleTimeOut());

        // Receive Protocol header
        processInput(transport, ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00}));

        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(remoteTimeoutHalf * 2));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(tick1);
        assertEquals("Unexpected deadline returned", expectedDeadline1, deadline);

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue (interimTick < expectedDeadline1);
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  expectedDeadline1, transport.tick(interimTick));
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        deadline = transport.tick(expectedDeadline1);
        assertEquals("When the deadline has been reached expected a new remote deadline to be returned", expectedDeadline2, deadline);
        assertEquals("tick() should have written data", 2, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));

        pumpMockTransport(transport);

        // Do some actual work, create real traffic, removing the need to send empty frame to satisfy idle-timeout
        connection.session().open();
        pumpMockTransport(transport);
        assertEquals("session open should have written data", 3, transport.writes.size());
        Object sentBeginFrame = transport.writes.get(2);
        assertNotNull("should have written a non-empty frame", sentBeginFrame);
        assertTrue("session open should have written a Begin frame", sentBeginFrame instanceof Begin);

        deadline = transport.tick(expectedDeadline2);
        assertEquals("When the deadline has been reached expected a new remote deadline to be returned", expectedDeadline3, deadline);
        assertEquals("tick() should not have written data as there was actual activity", 3, transport.writes.size());

        pumpMockTransport(transport);

        transport.tick(expectedDeadline3);
        assertEquals("tick() should have written data", 4, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));
    }

    @Test
    public void testTickWithBothTimeouts()
    {
        // all-positive
        doTickWithBothTimeoutsTestImpl(true, 5000, 2000, 10000, 12000, 14000, 15000);
        doTickWithBothTimeoutsTestImpl(false, 5000, 2000, 10000, 12000, 14000, 15000);

        // all-negative
        doTickWithBothTimeoutsTestImpl(true, 10000, 4000, -100000, -96000, -92000, -90000);
        doTickWithBothTimeoutsTestImpl(false, 10000, 4000, -100000, -96000, -92000, -90000);

        // negative to positive missing 0
        doTickWithBothTimeoutsTestImpl(true, 500, 200, -450, -250, -50, 50);
        doTickWithBothTimeoutsTestImpl(false, 500, 200, -450, -250, -50, 50);

        // negative to positive striking 0 with local deadline
        doTickWithBothTimeoutsTestImpl(true, 500, 200, -500, -300, -100, 1);
        doTickWithBothTimeoutsTestImpl(false, 500, 200, -500, -300, -100, 1);

        // negative to positive striking 0 with remote deadline
        doTickWithBothTimeoutsTestImpl(true, 500, 200, -200, 1, 201, 300);
        doTickWithBothTimeoutsTestImpl(false, 500, 200, -200, 1, 201, 300);
    }

    private void doTickWithBothTimeoutsTestImpl(boolean allowLocalTimeout, int localTimeout, int remoteTimeoutHalf, long tick1,
                                                long expectedDeadline1, long expectedDeadline2, long expectedDeadline3)
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Set our local idleTimeout
        transport.setIdleTimeout(localTimeout);

        connection.open();
        pumpMockTransport(transport);

        assertEquals("should have written data", 1, transport.writes.size());
        assertNotNull("should have written a non-empty frame", transport.writes.get(0));
        assertTrue("should have written an open frame", transport.writes.get(0) instanceof Open);

        // Receive Protocol header
        processInput(transport, ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00}));

        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(remoteTimeoutHalf * 2));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(tick1);
        assertEquals("Unexpected deadline returned", expectedDeadline1, deadline);

        // Wait for less time than the deadline with no data - get the same value
        long interimTick = tick1 + 10;
        assertTrue (interimTick < expectedDeadline1);
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  expectedDeadline1, transport.tick(interimTick));
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        deadline = transport.tick(expectedDeadline1);
        assertEquals("When the deadline has been reached expected a new remote deadline to be returned", expectedDeadline2, deadline);
        assertEquals("tick() should have written data", 2, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));

        pumpMockTransport(transport);

        deadline = transport.tick(expectedDeadline2);
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", expectedDeadline3, deadline);
        assertEquals("tick() should have written data", 3, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(2));

        pumpMockTransport(transport);

        if(allowLocalTimeout) {
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
            transport.tick(expectedDeadline3); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
            assertEquals("tick() should have written data", 4, transport.writes.size());
            assertNotNull("should have written a non-empty frame", transport.writes.get(3));
            assertTrue("should have written a close frame", transport.writes.get(3) instanceof Close);
        } else {
            // Receive Empty frame to satisfy local deadline
            processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

            deadline = transport.tick(expectedDeadline3);
            assertEquals("Receiving data should have reset the deadline (to the next remote one)",  expectedDeadline2 + (remoteTimeoutHalf), deadline);
            assertEquals("tick() shouldn't have written data", 3, transport.writes.size());
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemote()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteWithLocalTimeout()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsLocalThenRemoteTestImpl(boolean allowLocalTimeout) {
        int localTimeout = 5000;
        int remoteTimeoutHalf = 2000;
        assertTrue(remoteTimeoutHalf < localTimeout);

        long offset = 2500;
        assertTrue(offset < localTimeout);
        assertTrue(offset > remoteTimeoutHalf);

        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Set our local idleTimeout
        transport.setIdleTimeout(localTimeout);

        connection.open();
        pumpMockTransport(transport);

        assertEquals("should have written data", 1, transport.writes.size());
        assertNotNull("should have written a non-empty frame", transport.writes.get(0));
        assertTrue("should have written an open frame", transport.writes.get(0) instanceof Open);

        // Receive Protocol header
        processInput(transport, ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00}));

        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(remoteTimeoutHalf * 2));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(Long.MAX_VALUE - offset);
        assertEquals("Unexpected deadline returned", Long.MAX_VALUE - offset + remoteTimeoutHalf, deadline);

        deadline = transport.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  Long.MAX_VALUE -offset + remoteTimeoutHalf, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        deadline = transport.tick(Long.MAX_VALUE -offset + remoteTimeoutHalf); // Wait for the deadline - next deadline should be previous + remoteTimeoutHalf;
        assertEquals("When the deadline has been reached expected a new remote deadline to be returned", Long.MIN_VALUE + (2* remoteTimeoutHalf) - offset -1, deadline);
        assertEquals("tick() should have written data", 2, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));

        pumpMockTransport(transport);

        deadline = transport.tick(Long.MIN_VALUE + (2* remoteTimeoutHalf) - offset -1); // Wait for the deadline - next deadline should be orig + localTimeout;
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", Long.MIN_VALUE + (localTimeout - offset) -1, deadline);
        assertEquals("tick() should have written data", 3, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(2));

        pumpMockTransport(transport);

        if(allowLocalTimeout) {
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
            transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
            assertEquals("tick() should have written data", 4, transport.writes.size());
            assertNotNull("should have written a non-empty frame", transport.writes.get(3));
            assertTrue("should have written a close frame", transport.writes.get(3) instanceof Close);
        } else {
            // Receive Empty frame to satisfy local deadline
            processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

            deadline = transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline - next deadline should be orig + 3*remoteTimeoutHalf;
            assertEquals("Receiving data should have reset the deadline (to the remote one)",  Long.MIN_VALUE + (3* remoteTimeoutHalf) - offset -1, deadline);
            assertEquals("tick() shouldn't have written data", 3, transport.writes.size());
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocal()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalWithLocalTimeout()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsRemoteThenLocalTestImpl(boolean allowLocalTimeout) {
        int localTimeout = 2000;
        int remoteTimeoutHalf = 5000;
        assertTrue(localTimeout < remoteTimeoutHalf);

        long offset = 2500;
        assertTrue(offset > localTimeout);
        assertTrue(offset < remoteTimeoutHalf);

        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Set our local idleTimeout
        transport.setIdleTimeout(localTimeout);

        connection.open();
        pumpMockTransport(transport);

        assertEquals("should have written data", 1, transport.writes.size());
        assertNotNull("should have written a non-empty frame", transport.writes.get(0));
        assertTrue("should have written an open frame", transport.writes.get(0) instanceof Open);

        // Receive Protocol header
        processInput(transport, ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00}));

        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(remoteTimeoutHalf * 2));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(Long.MAX_VALUE - offset);
        assertEquals("Unexpected deadline returned",  Long.MAX_VALUE - offset + localTimeout, deadline);

        deadline = transport.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  Long.MAX_VALUE - offset + localTimeout, deadline);
        assertEquals("tick() shouldn't have written data", 1, transport.writes.size());

        // Receive Empty frame to satisfy local deadline
        processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

        deadline = transport.tick(Long.MAX_VALUE - offset + localTimeout); // Wait for the deadline - next deadline should be orig + 2* localTimeout;
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout, deadline);
        assertEquals("tick() should not have written data", 1, transport.writes.size());

        pumpMockTransport(transport);

        if(allowLocalTimeout) {
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
            transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
            assertEquals("tick() should have written data", 2, transport.writes.size());
            assertNotNull("should have written a non-empty frame", transport.writes.get(1));
            assertTrue("should have written a close frame", transport.writes.get(1) instanceof Close);
        } else {
            // Receive Empty frame to satisfy local deadline
            processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

            deadline = transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline - next deadline should be orig + remoteTimeoutHalf;
            assertEquals("Receiving data should have reset the deadline (to the remote one)",  Long.MIN_VALUE + remoteTimeoutHalf - offset -1, deadline);
            assertEquals("tick() shouldn't have written data", 1, transport.writes.size());

            deadline = transport.tick(Long.MIN_VALUE + remoteTimeoutHalf - offset -1); // Wait for the deadline - next deadline should be orig + 3* localTimeout;
            assertEquals("When the deadline has been reached expected a new local deadline to be returned", Long.MIN_VALUE + (3* localTimeout) - offset -1, deadline);
            assertEquals("tick() should have written data", 2, transport.writes.size());
            assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirst()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstWithLocalTimeout()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsBothRemoteFirstTestImpl(boolean allowLocalTimeout) {
        int localTimeout = 2000;
        int remoteTimeoutHalf = 2500;
        assertTrue(localTimeout < remoteTimeoutHalf);

        long offset = 500;
        assertTrue(offset < localTimeout);

        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Set our local idleTimeout
        transport.setIdleTimeout(localTimeout);

        connection.open();
        pumpMockTransport(transport);

        assertEquals("should have written data", 1, transport.writes.size());
        assertNotNull("should have written a non-empty frame", transport.writes.get(0));
        assertTrue("should have written an open frame", transport.writes.get(0) instanceof Open);

        // Receive Protocol header
        processInput(transport, ByteBuffer.wrap(new byte[] {'A', 'M', 'Q', 'P', 0x00, 0x01, 0x00, 0x00}));

        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(remoteTimeoutHalf * 2));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);

        long deadline = transport.tick(Long.MAX_VALUE - offset);
        assertEquals("Unexpected deadline returned",  Long.MIN_VALUE + (localTimeout - offset) -1, deadline);

        deadline = transport.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  Long.MIN_VALUE + (localTimeout - offset) -1, deadline);
        assertEquals("tick() shouldn't have written data", 1, transport.writes.size());

        // Receive Empty frame to satisfy local deadline
        processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

        deadline = transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline - next deadline should be orig + remoteTimeoutHalf;
        assertEquals("When the deadline has been reached expected a new remote deadline to be returned", Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        deadline = transport.tick(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1); // Wait for the deadline - next deadline should be orig + 2* localTimeout;
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout, deadline);
        assertEquals("tick() should have written data", 2, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));

        pumpMockTransport(transport);

        if(allowLocalTimeout) {
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
            transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
            assertEquals("tick() should have written data", 3, transport.writes.size());
            assertNotNull("should have written a non-empty frame", transport.writes.get(2));
            assertTrue("should have written a close frame", transport.writes.get(2) instanceof Close);
        } else {
            // Receive Empty frame to satisfy local deadline
            processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

            deadline = transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1 + localTimeout); // Wait for the deadline - next deadline should be orig + 2*remoteTimeoutHalf;
            assertEquals("Receiving data should have reset the deadline (to the remote one)",  Long.MIN_VALUE + (2* remoteTimeoutHalf) - offset -1, deadline);
            assertEquals("tick() shouldn't have written data", 2, transport.writes.size());
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
        }
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirst()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstTestImpl(false);
    }

    @Test
    public void testTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstWithLocalTimeout()
    {
        doTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstTestImpl(true);
    }

    private void doTickWithNanoTimeDerivedValueWhichWrapsBothLocalFirstTestImpl(boolean allowLocalTimeout) {
        int localTimeout = 5000;
        int remoteTimeoutHalf = 2000;
        assertTrue(remoteTimeoutHalf < localTimeout);

        long offset = 500;
        assertTrue(offset < remoteTimeoutHalf);

        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        // Set our local idleTimeout
        transport.setIdleTimeout(localTimeout);

        connection.open();
        while(transport.pending() > 0) {
            transport.pop(transport.head().remaining());
        }

        assertEquals("should have written data", 1, transport.writes.size());
        assertNotNull("should have written a non-empty frame", transport.writes.get(0));
        assertTrue("should have written an open frame", transport.writes.get(0) instanceof Open);

        // Handle the peer transmitting [half] their timeout. We half it on receipt to avoid spurious timeouts
        // if they not have transmitted half their actual timeout, as the AMQP spec only says they SHOULD do that.
        Open open = new Open();
        open.setIdleTimeOut(new UnsignedInteger(remoteTimeoutHalf * 2));
        TransportFrame openFrame = new TransportFrame(CHANNEL_ID, open, null);
        transport.handleFrame(openFrame);
        pumpMockTransport(transport);


        long deadline = transport.tick(Long.MAX_VALUE - offset);
        assertEquals("Unexpected deadline returned",  Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1, deadline);

        deadline = transport.tick(Long.MAX_VALUE - (offset - 100));    // Wait for less time than the deadline with no data - get the same value
        assertEquals("When the deadline hasn't been reached tick() should return the previous deadline",  Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1, deadline);
        assertEquals("When the deadline hasn't been reached tick() shouldn't write data", 1, transport.writes.size());

        deadline = transport.tick(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1); // Wait for the deadline - next deadline should be previous + remoteTimeoutHalf;
        assertEquals("When the deadline has been reached expected a new remote deadline to be returned", Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1 + remoteTimeoutHalf, deadline);
        assertEquals("tick() should have written data", 2, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(1));

        pumpMockTransport(transport);

        deadline = transport.tick(Long.MIN_VALUE + (remoteTimeoutHalf - offset) -1 + remoteTimeoutHalf); // Wait for the deadline - next deadline should be orig + localTimeout;
        assertEquals("When the deadline has been reached expected a new local deadline to be returned", Long.MIN_VALUE + (localTimeout - offset) -1, deadline);
        assertEquals("tick() should have written data", 3, transport.writes.size());
        assertEquals("tick() should have written an empty frame", null, transport.writes.get(2));

        pumpMockTransport(transport);

        if(allowLocalTimeout) {
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
            transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline, but don't receive traffic, allow local timeout to expire
            assertEquals("Calling tick() after the deadline should result in the connection being closed", EndpointState.CLOSED, connection.getLocalState());
            assertEquals("tick() should have written data", 4, transport.writes.size());
            assertNotNull("should have written a non-empty frame", transport.writes.get(3));
            assertTrue("should have written a close frame", transport.writes.get(3) instanceof Close);
        } else {
            // Receive Empty frame to satisfy local deadline
            processInput(transport,  ByteBuffer.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00}));

            deadline = transport.tick(Long.MIN_VALUE + (localTimeout - offset) -1); // Wait for the deadline - next deadline should be orig + 3*remoteTimeoutHalf;
            assertEquals("Receiving data should have reset the deadline (to the remote one)",  Long.MIN_VALUE + (3* remoteTimeoutHalf) - offset -1, deadline);
            assertEquals("tick() shouldn't have written data", 3, transport.writes.size());
            assertEquals("Connection should be active", EndpointState.ACTIVE, connection.getLocalState());
        }
    }

    @Test
    public void testMaxFrameSizeOfPeerHasEffect()
    {
        doMaxFrameSizeTestImpl(0, 0, 5700, 1);
        doMaxFrameSizeTestImpl(1024, 0, 5700, 6);
    }

    @Test
    public void testMaxFrameSizeOutgoingFrameSizeLimitHasEffect()
    {
        doMaxFrameSizeTestImpl(0, 512, 5700, 12);
        doMaxFrameSizeTestImpl(1024, 512, 5700, 12);
        doMaxFrameSizeTestImpl(1024, 2048, 5700, 6);
    }

    void doMaxFrameSizeTestImpl(int remoteMaxFrameSize, int outboundFrameSizeLimit, int contentLength, int expectedNumFrames)
    {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);

        // If we have been given an outboundFrameSizeLimit, configure it
        if(outboundFrameSizeLimit != 0) {
            transport.setOutboundFrameSizeLimit(outboundFrameSizeLimit);
        }

        Connection connection = Proton.connection();
        transport.bind(connection);

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        String messageContent = createLargeContent(contentLength);
        sendMessage(sender, "tag1", messageContent);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        // Now open the connection, expect the Open and Begin frames but
        // nothing else as we haven't opened the receiver itself yet.
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach then give sender credit
        Open open = new Open();
        if(remoteMaxFrameSize != 0) {
            open.setMaxFrameSize(UnsignedInteger.valueOf(remoteMaxFrameSize));
        }
        transport.handleFrame(new TransportFrame(0, open, null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setLinkCredit(UnsignedInteger.valueOf(10));

        transport.handleFrame(new TransportFrame(0, flow, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        // Now pump the transport again and expect transfers for the message
        pumpMockTransport(transport);

        // This calc isn't entirely precise, there is some added performative/frame overhead not
        // accounted for...but values are chosen to work, and verified here.
        final int frameCount;
        if(remoteMaxFrameSize == 0 && outboundFrameSizeLimit == 0) {
            frameCount = 1;
        } else if(remoteMaxFrameSize == 0 && outboundFrameSizeLimit != 0) {
            frameCount = (int) Math.ceil((double)contentLength / (double) outboundFrameSizeLimit);
        } else {
            int effectiveMaxFrameSize;
            if(outboundFrameSizeLimit != 0) {
                effectiveMaxFrameSize = Math.min(outboundFrameSizeLimit, remoteMaxFrameSize);
            } else {
                effectiveMaxFrameSize = remoteMaxFrameSize;
            }

            frameCount = (int) Math.ceil((double)contentLength / (double) effectiveMaxFrameSize);
        }

        assertEquals("Unexpected number of frames calculated", expectedNumFrames, frameCount);

        final int start = 3;
        final int totalExpected = start + frameCount;
        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), totalExpected, transport.writes.size());
        for(int i = start; i < totalExpected; i++) {
            assertTrue("Unexpected frame type", transport.writes.get(i) instanceof Transfer);
        }
    }

    private void processInput(MockTransportImpl transport, ByteBuffer data) {
        while (data.remaining() > 0)
        {
            int origLimit = data.limit();
            int amount = Math.min(transport.tail().remaining(), data.remaining());
            data.limit(data.position() + amount);
            transport.tail().put(data);
            data.limit(origLimit);
            transport.process();
        }
    }


    private static String createLargeContent(int length) {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return new String(payload, StandardCharsets.UTF_8);
    }

    @Test
    public void testMultiplexMultiFrameDeliveryOnSingleSessionOutgoing() {
        doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(false);
    }

    @Test
    public void testMultiplexMultiFrameDeliveriesOnSingleSessionOutgoing() {
        doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(true);
    }

    private void doMultiplexMultiFrameDeliveryOnSingleSessionOutgoingTestImpl(boolean bothDeliveriesMultiFrame) {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);

        int contentLength1 = 6000;
        int frameSizeLimit = 4000;
        int contentLength2 = 2000;
        if(bothDeliveriesMultiFrame) {
            contentLength2 = 6000;
        }

        Connection connection = Proton.connection();
        transport.bind(connection);

        Session session = connection.session();
        session.open();

        String linkName = "mySender1";
        Sender sender = session.sender(linkName);
        sender.open();

        String linkName2 = "mySender2";
        Sender sender2 = session.sender(linkName2);
        sender2.open();

        String messageContent1 = createLargeContent(contentLength1);
        sendMessage(sender, "tag1", messageContent1);

        String messageContent2 = createLargeContent(contentLength2);
        sendMessage(sender2, "tag2", messageContent2);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 0, transport.writes.size());

        // Now open the connection, expect the Open Begin, and Attach frames but
        // nothing else as we haven't remotely opened the receiver or given credit yet.
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Attach);

        // Send the necessary responses to open/begin/attach then give senders credit
        Open open = new Open();
        open.setMaxFrameSize(UnsignedInteger.valueOf(frameSizeLimit));

        transport.handleFrame(new TransportFrame(0, open, null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(UnsignedInteger.ZERO);
        attach1.setRole(Role.RECEIVER);
        attach1.setName(linkName);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);

        transport.handleFrame(new TransportFrame(0, attach1, null));

        Attach attach2 = new Attach();
        attach2.setHandle(UnsignedInteger.ONE);
        attach2.setRole(Role.RECEIVER);
        attach2.setName(linkName2);
        attach2.setInitialDeliveryCount(UnsignedInteger.ZERO);

        transport.handleFrame(new TransportFrame(0, attach2, null));

        Flow flow1 = new Flow();
        flow1.setHandle(UnsignedInteger.ZERO);
        flow1.setDeliveryCount(UnsignedInteger.ZERO);
        flow1.setNextIncomingId(UnsignedInteger.ONE);
        flow1.setNextOutgoingId(UnsignedInteger.ZERO);
        flow1.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow1.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow1.setLinkCredit(UnsignedInteger.valueOf(10));

        transport.handleFrame(new TransportFrame(0, flow1, null));

        Flow flow2 = new Flow();
        flow2.setHandle(UnsignedInteger.ONE);
        flow2.setDeliveryCount(UnsignedInteger.ZERO);
        flow2.setNextIncomingId(UnsignedInteger.ONE);
        flow2.setNextOutgoingId(UnsignedInteger.ZERO);
        flow2.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow2.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow2.setLinkCredit(UnsignedInteger.valueOf(10));

        transport.handleFrame(new TransportFrame(0, flow2, null));

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        // Now pump the transport again and expect transfers for the messages
        pumpMockTransport(transport);

        int expectedFrames = bothDeliveriesMultiFrame ? 8 : 7;
        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), expectedFrames, transport.writes.size());

        FrameBody frameBody = transport.writes.get(4);
        assertTrue("Unexpected frame type", frameBody instanceof Transfer);
        Transfer transfer = (Transfer) frameBody;
        assertEquals("Unexpected delivery tag", new Binary("tag1".getBytes(StandardCharsets.UTF_8)), transfer.getDeliveryTag());
        assertEquals("Unexpected deliveryId", UnsignedInteger.ZERO, transfer.getDeliveryId());
        assertEquals("Unexpected more flag", true, transfer.getMore());

        frameBody = transport.writes.get(5);
        assertTrue("Unexpected frame type", frameBody instanceof Transfer);
        transfer = (Transfer) frameBody;
        assertEquals("Unexpected delivery tag", new Binary("tag2".getBytes(StandardCharsets.UTF_8)), transfer.getDeliveryTag());
        assertEquals("Unexpected deliveryId", UnsignedInteger.ONE, transfer.getDeliveryId());
        assertEquals("Unexpected more flag", bothDeliveriesMultiFrame, transfer.getMore());

        frameBody = transport.writes.get(6);
        assertTrue("Unexpected frame type", frameBody instanceof Transfer);
        transfer = (Transfer) frameBody;
        assertEquals("Unexpected delivery tag", new Binary("tag1".getBytes(StandardCharsets.UTF_8)), transfer.getDeliveryTag());
        assertEquals("Unexpected deliveryId", UnsignedInteger.ZERO, transfer.getDeliveryId());
        assertEquals("Unexpected more flag", false, transfer.getMore());

        if(bothDeliveriesMultiFrame) {
            frameBody = transport.writes.get(7);
            assertTrue("Unexpected frame type", frameBody instanceof Transfer);
            transfer = (Transfer) frameBody;
            assertEquals("Unexpected delivery tag", new Binary("tag2".getBytes(StandardCharsets.UTF_8)), transfer.getDeliveryTag());
            assertEquals("Unexpected deliveryId", UnsignedInteger.ONE, transfer.getDeliveryId());
            assertEquals("Unexpected more flag", false, transfer.getMore());
        }
    }

    @Test
    public void testMultiplexMultiFrameDeliveriesOnSingleSessionIncoming() {
        doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(true);
    }

    @Test
    public void testMultiplexMultiFrameDeliveryOnSingleSessionIncoming() {
        doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(false);
    }

    private void doMultiplexMultiFrameDeliveryOnSingleSessionIncomingTestImpl(boolean bothDeliveriesMultiFrame) {
        int contentLength1 = 7000;
        int maxPayloadChunkSize = 2000;
        int contentLength2 = 1000;
        if(bothDeliveriesMultiFrame) {
            contentLength2 = 4100;
        }

        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        String linkName2 = "myReceiver2";
        Receiver receiver2 = session.receiver(linkName2);
        receiver2.flow(5);
        receiver2.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;
        final UnsignedInteger r2handle = UnsignedInteger.ONE;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        FrameBody frame = transport.writes.get(2);
        assertTrue("Unexpected frame type", frame instanceof Attach);
        assertEquals("Unexpected handle", ((Attach) frame).getHandle(), r1handle);
        frame = transport.writes.get(3);
        assertTrue("Unexpected frame type", frame instanceof Attach);
        assertEquals("Unexpected handle", ((Attach) frame).getHandle(), r2handle);
        frame = transport.writes.get(4);
        assertTrue("Unexpected frame type", frame instanceof Flow);
        assertEquals("Unexpected handle", ((Flow) frame).getHandle(), r1handle);
        frame = transport.writes.get(5);
        assertTrue("Unexpected frame type", frame instanceof Flow);
        assertEquals("Unexpected handle", ((Flow) frame).getHandle(), r2handle);

        assertNull("Should not yet have a delivery", receiver1.current());
        assertNull("Should not yet have a delivery", receiver2.current());

        // Send the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        Attach attach2 = new Attach();
        attach2.setHandle(r2handle);
        attach2.setRole(Role.SENDER);
        attach2.setName(linkName2);
        attach2.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach2, null));

        String deliveryTag1 = "tag1";
        String messageContent1 = createLargeContent(contentLength1);
        String deliveryTag2 = "tag2";
        String messageContent2 = createLargeContent(contentLength2);

        ArrayList<byte[]> message1chunks = createTransferPayloads(messageContent1, maxPayloadChunkSize);
        assertEquals("unexpected number of payload chunks", 4, message1chunks.size());
        ArrayList<byte[]> message2chunks = createTransferPayloads(messageContent2, maxPayloadChunkSize);
        if(bothDeliveriesMultiFrame) {
            assertEquals("unexpected number of payload chunks", 3, message2chunks.size());
        } else {
            assertEquals("unexpected number of payload chunks", 1, message2chunks.size());
        }

        while (true) {
           if (!message1chunks.isEmpty()) {
              byte[] chunk = message1chunks.remove(0);
              handlePartialTransfer(transport, r1handle, 1, deliveryTag1, chunk, !message1chunks.isEmpty());
           }

           if (!message2chunks.isEmpty()) {
               byte[] chunk = message2chunks.remove(0);
               handlePartialTransfer(transport, r2handle, 2, deliveryTag2, chunk, !message2chunks.isEmpty());
            }

           if (message1chunks.isEmpty() && message2chunks.isEmpty()) {
              break;
           }
        }

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());

        assertEquals("Unexpected queued count", 1, receiver1.getQueued());
        Delivery delivery1 = verifyDelivery(receiver1, deliveryTag1, messageContent1);
        assertNotNull("Should now have a delivery", delivery1);
        assertEquals("Unexpected queued count", 0, receiver1.getQueued());

        assertEquals("Unexpected queued count", 1, receiver2.getQueued());
        Delivery delivery2 = verifyDelivery(receiver2, deliveryTag2, messageContent2);
        assertNotNull("Should now have a delivery", delivery2);
        assertEquals("Unexpected queued count", 0, receiver2.getQueued());

        delivery1.disposition(Accepted.getInstance());
        delivery1.settle();
        pumpMockTransport(transport);
        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 7, transport.writes.size());

        frame = transport.writes.get(6);
        assertTrue("Unexpected frame type", frame instanceof Disposition);
        assertEquals("Unexpected delivery id", ((Disposition) frame).getFirst(), UnsignedInteger.ONE);
        assertEquals("Unexpected delivery id", ((Disposition) frame).getLast(), UnsignedInteger.ONE);

        delivery2.disposition(Accepted.getInstance());
        delivery2.settle();
        pumpMockTransport(transport);
        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 8, transport.writes.size());

        frame = transport.writes.get(7);
        assertTrue("Unexpected frame type", frame instanceof Disposition);
        assertEquals("Unexpected delivery id", ((Disposition) frame).getFirst(), UnsignedInteger.valueOf(2));
        assertEquals("Unexpected delivery id", ((Disposition) frame).getLast(), UnsignedInteger.valueOf(2));
    }

    private void handleTransfer(TransportImpl transport, UnsignedInteger handle, int deliveryId, String deliveryTag, byte[] payload)
    {
        handlePartialTransfer(transport, handle, UnsignedInteger.valueOf(deliveryId), deliveryTag, payload, false);
    }

    private void handlePartialTransfer(TransportImpl transport, UnsignedInteger handle, int deliveryId, String deliveryTag, byte[] partialPayload, boolean more)
    {
        handlePartialTransfer(transport, handle, UnsignedInteger.valueOf(deliveryId), deliveryTag, partialPayload, more);
    }

    private void handlePartialTransfer(TransportImpl transport, UnsignedInteger handle, UnsignedInteger deliveryId, String deliveryTag, byte[] partialPayload, boolean more)
    {
        handlePartialTransfer(transport, handle, deliveryId, deliveryTag, partialPayload, more, false);
    }

    private void handlePartialTransfer(TransportImpl transport, UnsignedInteger handle, UnsignedInteger deliveryId, String deliveryTag, byte[] partialPayload, boolean more, boolean aborted)
    {
        handlePartialTransfer(transport, handle, deliveryId, deliveryTag, partialPayload, more, aborted, null);
    }

    private void handlePartialTransfer(TransportImpl transport, UnsignedInteger handle, UnsignedInteger deliveryId, String deliveryTag, byte[] partialPayload, boolean more, boolean aborted, Boolean settled)
    {
        byte[] tag = deliveryTag.getBytes(StandardCharsets.UTF_8);

        Transfer transfer = new Transfer();
        transfer.setHandle(handle);
        transfer.setDeliveryTag(new Binary(tag));
        transfer.setMessageFormat(UnsignedInteger.valueOf(DeliveryImpl.DEFAULT_MESSAGE_FORMAT));
        transfer.setMore(more);
        transfer.setAborted(aborted);
        if(deliveryId != null) {
            // Can be omitted in continuation frames for a given delivery.
            transfer.setDeliveryId(deliveryId);
        }
        if(settled != null) {
            transfer.setSettled(settled);
        }

        transport.handleFrame(new TransportFrame(0, transfer, new Binary(partialPayload, 0, partialPayload.length)));
    }

    private ArrayList<byte[]> createTransferPayloads(String content, int payloadChunkSize)
    {
        ArrayList<byte[]> payloadChunks = new ArrayList<>();

        Message m = Message.Factory.create();
        m.setBody(new AmqpValue(content));

        byte[] encoded = new byte[BUFFER_SIZE];
        int len = m.encode(encoded, 0, BUFFER_SIZE);
        assertTrue("given array was too small", len < BUFFER_SIZE);

        int copied = 0;
        while(copied < len) {
            int chunkSize = Math.min(len - copied, payloadChunkSize);
            byte[] chunk = new byte[chunkSize];

            System.arraycopy(encoded, copied, chunk, 0, chunkSize);

            payloadChunks.add(chunk);
            copied += chunkSize;
        }

        assertFalse("no payload chunks to return", payloadChunks.isEmpty());

        return payloadChunks;
    }

    @Test
    public void testDeliveryIdOutOfSequenceCausesISE() {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        String linkName2 = "myReceiver2";
        Receiver receiver2 = session.receiver(linkName2);
        receiver2.flow(5);
        receiver2.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;
        final UnsignedInteger r2handle = UnsignedInteger.ONE;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        Attach attach2 = new Attach();
        attach2.setHandle(r2handle);
        attach2.setRole(Role.SENDER);
        attach2.setName(linkName2);
        attach2.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach2, null));

        String deliveryTag1 = "tag1";
        String deliveryTag2 = "tag2";

        handlePartialTransfer(transport, r2handle, 2, deliveryTag2, new byte[]{ 2 }, false);
        try {
            handlePartialTransfer(transport, r1handle, 1, deliveryTag1, new byte[]{ 1 }, false);
            fail("Expected an ISE");
        } catch(IllegalStateException ise) {
            // Expected
            assertTrue("Unexpected exception:" + ise, ise.getMessage().contains("Expected delivery-id 3, got 1"));
        }
    }

    @Test
    public void testDeliveryIdMissingOnInitialTransferCausesISE() {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        // Receive a delivery without any delivery-id on the [first] transfer frame, expect it to fail.
        try {
            handlePartialTransfer(transport, r1handle, null, "tag1", new byte[]{ 1 }, false);
            fail("Expected an ISE");
        } catch(IllegalStateException ise) {
            // Expected
            assertEquals("Unexpected message", "No delivery-id specified on first Transfer of new delivery", ise.getMessage());
        }
    }

    @Test
    public void testMultiplexDeliveriesOnSameReceiverLinkCausesISE() {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        // Receive first transfer for a multi-frame delivery
        handlePartialTransfer(transport, r1handle, 1, "tag1", new byte[]{ 1 }, true);

        // Receive first transfer for ANOTHER multi-frame delivery, expect it to fail
        // as multiplexing deliveries on a single link is forbidden by the spec.
        try {
            handlePartialTransfer(transport, r1handle, 2, "tag2", new byte[]{ 2 }, true);
            fail("Expected an ISE");
        } catch(IllegalStateException ise) {
            // Expected
            assertEquals("Unexpected message", "Illegal multiplex of deliveries on same link with delivery-id 1 and 2", ise.getMessage());
        }
    }

    @Test
    public void testDeliveryIdTrackingHandlesAbortedDelivery() {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        // Receive first transfer for a multi-frame delivery
        assertEquals("Unexpected queued count", 0, receiver1.getQueued());
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ZERO, "tag1", new byte[]{ 1 }, true);
        assertEquals("Unexpected queued count", 1, receiver1.getQueued());
        // Receive second transfer for a multi-frame delivery, indicating it is aborted
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ZERO, "tag1", new byte[]{ 2 }, true, true);
        assertEquals("Unexpected queued count", 1, receiver1.getQueued());

        // Receive first transfer for ANOTHER delivery, expect it not to fail, since the earlier delivery aborted
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ONE, "tag2", new byte[]{ 3 }, false);
        assertEquals("Unexpected queued count", 2, receiver1.getQueued());

        receiver1.advance();
        verifyDeliveryRawPayload(receiver1, "tag2", new byte[] { 3 });
    }

    @Test
    public void testDeliveryWithIdOmittedOnContinuationTransfers() {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        String linkName2 = "myReceiver2";
        Receiver receiver2 = session.receiver(linkName2);
        receiver2.flow(5);
        receiver2.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;
        final UnsignedInteger r2handle = UnsignedInteger.ONE;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        Attach attach2 = new Attach();
        attach2.setHandle(r2handle);
        attach2.setRole(Role.SENDER);
        attach2.setName(linkName2);
        attach2.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach2, null));

        String deliveryTag1 = "tag1";
        String deliveryTag2 = "tag2";

        // Send multi-frame deliveries for each link, multiplexed together, and omit
        // the delivery-id on the continuation frames as allowed for by the spec.
        handlePartialTransfer(transport, r1handle, 1, deliveryTag1, new byte[]{ 1 }, true);
        handlePartialTransfer(transport, r2handle, 2, deliveryTag2, new byte[]{ 101 }, true);
        handlePartialTransfer(transport, r2handle, null, deliveryTag2, new byte[]{ 102 }, true);
        handlePartialTransfer(transport, r1handle, null, deliveryTag1, new byte[]{ 2 }, true);
        handlePartialTransfer(transport, r1handle, null, deliveryTag1, new byte[]{ 3 }, false);
        handlePartialTransfer(transport, r2handle, null, deliveryTag2, new byte[]{ 103 }, true);
        handlePartialTransfer(transport, r2handle, null, deliveryTag2, new byte[]{ 104 }, false);

        // Verify the transfer frames were all matched to compose the expected delivery payload.
        verifyDeliveryRawPayload(receiver1, deliveryTag1, new byte[] { 1, 2, 3 });
        verifyDeliveryRawPayload(receiver2, deliveryTag2, new byte[] { 101, 102, 103, 104 });
    }

    @Test
    public void testDeliveryIdThresholdsAndWraps() {
        // Check start from 0
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.ZERO, UnsignedInteger.ONE, UnsignedInteger.valueOf(2));
        // Check run up to max-int (interesting boundary for underlying impl)
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(Integer.MAX_VALUE - 2), UnsignedInteger.valueOf(Integer.MAX_VALUE -1), UnsignedInteger.valueOf(Integer.MAX_VALUE));
        // Check crossing from signed range value into unsigned range value (interesting boundary for underlying impl)
        long maxIntAsLong = Integer.MAX_VALUE;
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(maxIntAsLong), UnsignedInteger.valueOf(maxIntAsLong + 1L), UnsignedInteger.valueOf(maxIntAsLong + 2L));
        // Check run up to max-uint
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.valueOf(0xFFFFFFFFL - 2), UnsignedInteger.valueOf(0xFFFFFFFFL - 1), UnsignedInteger.MAX_VALUE);
        // Check wrapping from max unsigned value back to min(/0).
        doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger.MAX_VALUE, UnsignedInteger.ZERO, UnsignedInteger.ONE);
    }

    private void doDeliveryIdThresholdsWrapsTestImpl(UnsignedInteger deliveryId1, UnsignedInteger deliveryId2, UnsignedInteger deliveryId3) {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(5);
        receiver1.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        String deliveryTag1 = "tag1";
        String deliveryTag2 = "tag2";
        String deliveryTag3 = "tag3";

        // Send deliveries with the given delivery-id
        handlePartialTransfer(transport, r1handle, deliveryId1, deliveryTag1, new byte[]{ 1 }, false);
        handlePartialTransfer(transport, r1handle, deliveryId2, deliveryTag2, new byte[]{ 2 }, false);
        handlePartialTransfer(transport, r1handle, deliveryId3, deliveryTag3, new byte[]{ 3 }, false);

        // Verify deliveries arrived with expected payload
        verifyDeliveryRawPayload(receiver1, deliveryTag1, new byte[] { 1 });
        verifyDeliveryRawPayload(receiver1, deliveryTag2, new byte[] { 2 });
        verifyDeliveryRawPayload(receiver1, deliveryTag3, new byte[] { 3 });
    }

    @Test
    public void testAbortedDelivery() {
        // Check aborted=true, more=false, settled=true.
        doAbortedDeliveryTestImpl(false, true);
        // Check aborted=true, more=false, settled=unset(false)
        // Aborted overrides settled not being set.
        doAbortedDeliveryTestImpl(false, null);
        // Check aborted=true, more=false, settled=false
        // Aborted overrides settled being explicitly false.
        doAbortedDeliveryTestImpl(false, false);

        // Check aborted=true, more=true, settled=true
        // Aborted overrides the more=true.
        doAbortedDeliveryTestImpl(true, true);
        // Check aborted=true, more=true, settled=unset(false)
        // Aborted overrides the more=true, and settled being unset.
        doAbortedDeliveryTestImpl(true, null);
        // Check aborted=true, more=true, settled=false
        // Aborted overrides the more=true, and settled explicitly false.
        doAbortedDeliveryTestImpl(true, false);
    }

    private void doAbortedDeliveryTestImpl(boolean setMoreOnAbortedTransfer, Boolean setSettledOnAbortedTransfer) {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName1 = "myReceiver1";
        Receiver receiver1 = session.receiver(linkName1);
        receiver1.flow(3);
        receiver1.open();

        pumpMockTransport(transport);

        final UnsignedInteger r1handle = UnsignedInteger.ZERO;

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        // Give the necessary responses to open/begin/attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach1 = new Attach();
        attach1.setHandle(r1handle);
        attach1.setRole(Role.SENDER);
        attach1.setName(linkName1);
        attach1.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach1, null));

        String deliveryTag1 = "tag1";
        String deliveryTag2 = "tag2";
        String deliveryTag3 = "tag3";

        // Receive first delivery
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ZERO, deliveryTag1, new byte[]{ 1 }, true);
        assertEquals("Unexpected incoming bytes count", 1, session.getIncomingBytes());
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ZERO, deliveryTag1, new byte[]{ 2 }, false);

        assertEquals("Unexpected queued count", 1, receiver1.getQueued());
        assertEquals("Unexpected incoming bytes count", 2, session.getIncomingBytes());
        assertEquals("Unexpected credit", 3, receiver1.getCredit());

        // Receive first transfer for a multi-frame delivery
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ONE, deliveryTag2, new byte[]{ 3 }, true);
        assertEquals("Unexpected queued count", 2, receiver1.getQueued());
        assertEquals("Unexpected credit", 3, receiver1.getCredit());
        assertEquals("Unexpected incoming bytes count", 3, session.getIncomingBytes());
        // Receive second transfer for a multi-frame delivery, indicating it is aborted
        handlePartialTransfer(transport, r1handle, UnsignedInteger.ONE, deliveryTag2, new byte[]{ 4 }, setMoreOnAbortedTransfer, true, setSettledOnAbortedTransfer);
        assertEquals("Unexpected queued count", 2, receiver1.getQueued());
        assertEquals("Unexpected credit", 3, receiver1.getCredit());
        // The aborted frame payload, if any, is dropped. Earlier payload could have already been read, was
        // previously accounted for, and is incomplete, leaving alone for regular cleanup accounting handling.
        assertEquals("Unexpected incoming bytes count", 3, session.getIncomingBytes());

        // Receive transfers for ANOTHER delivery, expect it not to fail, since the earlier delivery aborted
        handlePartialTransfer(transport, r1handle, UnsignedInteger.valueOf(2), deliveryTag3, new byte[]{ 5 }, true);
        handlePartialTransfer(transport, r1handle, UnsignedInteger.valueOf(2), deliveryTag3, new byte[]{ 6 }, false);
        assertEquals("Unexpected queued count", 3, receiver1.getQueued());
        assertEquals("Unexpected credit", 3, receiver1.getCredit());
        assertEquals("Unexpected incoming bytes count", 5, session.getIncomingBytes());

        // Check the first delivery
        verifyDeliveryRawPayload(receiver1, deliveryTag1, new byte[] { 1, 2 });
        assertEquals("Unexpected queued count", 2, receiver1.getQueued());
        assertEquals("Unexpected credit", 2, receiver1.getCredit());
        assertEquals("Unexpected incoming bytes count", 3, session.getIncomingBytes());

        // Check the aborted delivery
        Delivery delivery = receiver1.current();
        assertTrue(Arrays.equals(deliveryTag2.getBytes(StandardCharsets.UTF_8), delivery.getTag()));

        assertTrue(delivery.isAborted());
        assertTrue(delivery.remotelySettled()); // Since aborted implicitly means it is settled.
        assertTrue(delivery.isPartial());
        assertTrue(delivery.isReadable());

        byte[] received = new byte[delivery.pending()];
        int len = receiver1.recv(received, 0, BUFFER_SIZE);
        assertEquals("unexpected length", len, received.length);

        assertArrayEquals("Received delivery payload not as expected", new byte[] { 3 }, received);

        assertTrue("receiver did not advance", receiver1.advance());

        assertEquals("Unexpected queued count", 1, receiver1.getQueued());
        assertEquals("Unexpected credit", 1, receiver1.getCredit());
        assertEquals("Unexpected incoming bytes count", 2, session.getIncomingBytes());

        // Check the third delivery
        verifyDeliveryRawPayload(receiver1, deliveryTag3, new byte[] { 5, 6 });
        assertEquals("Unexpected queued count", 0, receiver1.getQueued());
        assertEquals("Unexpected credit", 0, receiver1.getCredit());
        assertEquals("Unexpected incoming bytes count", 0, session.getIncomingBytes());

        // Flow new credit and check delivery-count + credit on wire are as expected.
        receiver1.flow(123);
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Flow);
        Flow sentFlow = (Flow) transport.writes.get(4);

        assertEquals("Unexpected delivery count", UnsignedInteger.valueOf(3), sentFlow.getDeliveryCount());
        assertEquals("Unexpected credit", UnsignedInteger.valueOf(123), sentFlow.getLinkCredit());
    }

    @Test
    public void testErrorConditionDefault() {
        TransportImpl transport = new TransportImpl();
        assertNull("Expected null ErrorCondition given historic behaviour", transport.getCondition());
    }

    @Test
    public void testErrorConditionSetGet() {
        // Try setting with an empty condition object, expect to get a null back per historic behaviour.
        TransportImpl transport = new TransportImpl();

        ErrorCondition emptyErrorCondition = new ErrorCondition();
        assertNull("Expected empty Condition given historic behaviour", emptyErrorCondition.getCondition());
        transport.setCondition(emptyErrorCondition);
        assertNull("Expected null ErrorCondition given historic behaviour", transport.getCondition());

        // Try setting with a populated condition object.
        transport = new TransportImpl();

        Symbol condition = Symbol.getSymbol("some-error");
        String description = "some-error-description";
        ErrorCondition populatedErrorCondition = new ErrorCondition();
        populatedErrorCondition.setCondition(condition);
        populatedErrorCondition.setDescription(description);
        assertNotNull("Expected a Condition", populatedErrorCondition.getCondition());

        transport.setCondition(populatedErrorCondition);
        assertNotNull("Expected an ErrorCondition to be returned", transport.getCondition());
        assertEquals("Unexpected ErrorCondition returned", populatedErrorCondition, transport.getCondition());

        // Try setting again with another populated condition object.
        Symbol otherCondition = Symbol.getSymbol("some-other-error");
        String otherDescription = "some-other-error-description";
        ErrorCondition otherErrorCondition = new ErrorCondition();
        otherErrorCondition.setCondition(otherCondition);
        otherErrorCondition.setDescription(otherDescription);
        assertNotNull("Expected a Condition", otherErrorCondition.getCondition());

        assertNotEquals(condition, otherCondition);
        assertNotEquals(populatedErrorCondition.getCondition(), otherErrorCondition.getCondition());
        assertNotEquals(description, otherDescription);
        assertNotEquals(populatedErrorCondition.getDescription(), otherErrorCondition.getDescription());
        assertNotEquals(populatedErrorCondition, otherErrorCondition);

        transport.setCondition(otherErrorCondition);
        assertNotNull("Expected an ErrorCondition to be returned", transport.getCondition());
        assertEquals("Unexpected ErrorCondition returned", otherErrorCondition, transport.getCondition());

        // Try setting again with an empty condition object, expect to get a null back per historic behaviour.
        transport.setCondition(emptyErrorCondition);
        assertNull("Expected null ErrorCondition given historic behaviour", transport.getCondition());
    }

    @Test
    public void testErrorConditionAfterTransportClosed() {
        Symbol condition = Symbol.getSymbol("some-error");
        String description = "some-error-description";
        ErrorCondition origErrorCondition = new ErrorCondition();
        origErrorCondition.setCondition(condition);
        origErrorCondition.setDescription(description);
        assertNotNull("Expected a Condition", origErrorCondition.getCondition());

        // Set an error condition, then call 'closed' specifying an error.
        // Expect the original condition which was set to remain.
        TransportImpl transport = new TransportImpl();

        transport.setCondition(origErrorCondition);
        transport.closed(new TransportException("my-ignored-exception"));

        assertNotNull("Expected an ErrorCondition to be returned", transport.getCondition());
        assertEquals("Unexpected ErrorCondition returned", origErrorCondition, transport.getCondition());

        // ---------------------------------------------------------------- //

        // Set an error condition, then call 'closed' without an error.
        // Expect the original condition which was set to remain.
        transport = new TransportImpl();

        transport.setCondition(origErrorCondition);
        transport.closed(null);

        assertNotNull("Expected an ErrorCondition to be returned", transport.getCondition());
        assertEquals("Unexpected ErrorCondition returned", origErrorCondition, transport.getCondition());

        // ---------------------------------------------------------------- //

        // Without having set an error condition, call 'closed' specifying an error.
        // Expect a condition to be set.
        transport = new TransportImpl();
        transport.closed(new TransportException(description));

        assertNotNull("Expected an ErrorCondition to be returned", transport.getCondition());
        assertEquals("Unexpected condition returned", ConnectionError.FRAMING_ERROR, transport.getCondition().getCondition());
        assertEquals("Unexpected description returned", "org.apache.qpid.proton.engine.TransportException: " + description, transport.getCondition().getDescription());

        // ---------------------------------------------------------------- //

        // Without having set an error condition, call 'closed' without an error.
        // Expect a condition to be set.
        transport = new TransportImpl();

        transport.closed(null);

        assertNotNull("Expected an ErrorCondition to be returned", transport.getCondition());
        assertEquals("Unexpected ErrorCondition returned", ConnectionError.FRAMING_ERROR, transport.getCondition().getCondition());
        assertEquals("Unexpected description returned", "connection aborted", transport.getCondition().getDescription());
    }

    @Test
    public void testCloseFrameErrorAfterTransportClosed() {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        prepareAndOpenConnection(transport, connection);

        Symbol condition = Symbol.getSymbol("some-error");
        String description = "some-error-description";
        ErrorCondition origErrorCondition = new ErrorCondition();
        origErrorCondition.setCondition(condition);
        origErrorCondition.setDescription(description);
        assertNotNull("Expected a Condition", origErrorCondition.getCondition());

        // Set an error condition, then call 'closed' specifying an error.
        // Expect the original condition which was set to be emitted
        // in the close frame generated.

        transport.setCondition(origErrorCondition);
        transport.closed(new TransportException("my-ignored-exception"));

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        FrameBody frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);
        assertEquals("Unexpected condition", origErrorCondition, ((Close) frameBody).getError());

        // ---------------------------------------------------------------- //

        // Set an error condition, then call 'closed' without an error.
        // Expect the original condition which was set to be emitted
        // in the close frame generated.

        transport = new MockTransportImpl();
        connection = Proton.connection();
        prepareAndOpenConnection(transport, connection);

        transport.setCondition(origErrorCondition);
        transport.closed(null);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);
        assertEquals("Unexpected condition", origErrorCondition, ((Close) frameBody).getError());

        // ---------------------------------------------------------------- //

        // Without having set an error condition, call 'closed' specifying an error.
        // Expect a condition to be emitted in the close frame generated.
        transport = new MockTransportImpl();
        connection = Proton.connection();
        prepareAndOpenConnection(transport, connection);

        transport.closed(new TransportException(description));

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);
        ErrorCondition expectedCondition = new ErrorCondition(ConnectionError.FRAMING_ERROR, "org.apache.qpid.proton.engine.TransportException: " + description);
        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());

        // ---------------------------------------------------------------- //

        // Without having set an error condition, call 'closed' without an error.
        // Expect a condition to be emitted in the close frame generated.
        transport = new MockTransportImpl();
        connection = Proton.connection();
        prepareAndOpenConnection(transport, connection);

        transport.closed(null);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);
        expectedCondition = new ErrorCondition(ConnectionError.FRAMING_ERROR, "connection aborted");
        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());

        // ---------------------------------------------------------------- //

        // Without having set an error condition on the transport, call 'closed' with an error,
        // but then also set a condition on the connection, and expect the connection error
        // condition to be emitted in the close frame generated.
        transport = new MockTransportImpl();
        connection = Proton.connection();
        prepareAndOpenConnection(transport, connection);

        transport.closed(new TransportException("some other transport exception"));
        connection.setCondition(origErrorCondition);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);
        assertEquals("Unexpected condition", origErrorCondition, ((Close) frameBody).getError());
    }

    @Test
    public void testCloseFrameErrorAfterDecodeError() {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        transport.bind(connection);
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);

        assertEvents(collector, Event.Type.CONNECTION_INIT, Event.Type.CONNECTION_BOUND, Event.Type.CONNECTION_LOCAL_OPEN, Event.Type.TRANSPORT);

        // Provide the response bytes for the header
        transport.tail().put(AmqpHeader.HEADER);
        transport.process();

        // Provide the bytes for Open, but omit the mandatory container-id to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
                                     0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                                     0x00, 0x53, 0x10, 0x45};// Described-type, ulong type, open descriptor, list0.

        int capacity = transport.capacity();
        assertTrue("Unexpected transport capacity: " + capacity, capacity > bytes.length);

        transport.tail().put(bytes);
        transport.process();

        assertEvents(collector, Event.Type.TRANSPORT_ERROR, Event.Type.TRANSPORT_TAIL_CLOSED);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        FrameBody frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);

        // Expect the close frame generated to contain a decode error condition referencing the missing container-id.
        ErrorCondition expectedCondition = new ErrorCondition();
        expectedCondition.setCondition(AmqpError.DECODE_ERROR);
        expectedCondition.setDescription("The container-id field cannot be omitted");

        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());
    }

    @Test
    public void testEmptyBeginProvokesDecodeError() {
        // Provide the bytes for Begin, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x11, 0x45};// Described-type, ulong type, Begin descriptor, list0.

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test
    public void testTruncatedBeginProvokesDecodeError1() {
        // Provide the bytes for Begin, but only give a null (i-e not-present) for the remote-channel.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x11, (byte) 0xC0, // Described-type, ulong type, Begin descriptor, list8.
                0x03, 0x01, 0x40 }; // size (3), count (1), remote-channel (null).

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test
    public void testTruncatedBeginProvokesDecodeError2() {
        // Provide the bytes for Begin, but only give a [not-present remote-channel +] next-outgoing-id and incoming-window. Provokes a decode error as there must be 4 fields.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x11, // Frame size = 17 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x11, (byte) 0xC0, // Described-type, ulong type, Begin descriptor, list8.
                0x05, 0x03, 0x40, 0x43, 0x43 }; // size (5), count (3), remote-channel (null), next-outgoing-id (uint0), incoming-window (uint0).

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test
    public void testTruncatedBeginProvokesDecodeError3() {
        // Provide the bytes for Begin, but only give a [not-present remote-channel +] next-outgoing-id and incoming-window, and send not-present/null for outgoing-window. Provokes a decode error as must be present.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x12, // Frame size = 18 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x11, (byte) 0xC0, // Described-type, ulong type, Begin descriptor, list8.
                0x06, 0x04, 0x40, 0x43, 0x43, 0x40 }; // size (5), count (4), remote-channel (null), next-outgoing-id (uint0), incoming-window (uint0), outgoing-window (null).

        doInvalidBeginProvokesDecodeErrorTestImpl(bytes, "Unexpected null value - mandatory field not set? (the outgoing-window field is mandatory)");
    }

    private void doInvalidBeginProvokesDecodeErrorTestImpl(byte[] bytes, String description) {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        transport.bind(connection);
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);

        // Provide the response bytes for the header
        transport.tail().put(AmqpHeader.HEADER);
        transport.process();

        // Send the necessary response to open
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        int capacity = transport.capacity();
        assertTrue("Unexpected transport capacity: " + capacity, capacity > bytes.length);

        transport.tail().put(bytes);
        transport.process();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        FrameBody frameBody = transport.writes.get(1);
        assertTrue("Unexpected frame type", frameBody instanceof Close);

        // Expect the close frame generated to contain a decode error condition referencing the missing container-id.
        ErrorCondition expectedCondition = new ErrorCondition();
        expectedCondition.setCondition(AmqpError.DECODE_ERROR);
        expectedCondition.setDescription(description);

        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());
    }

    @Test
    public void testEmptyFlowProvokesDecodeError() {
        // Provide the bytes for Flow, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x13, 0x45};// Described-type, ulong type, Flow descriptor, list0.

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test
    public void testTruncatedFlowProvokesDecodeError1() {
        // Provide the bytes for Flow, but only give a 0 for the next-incoming-id. Provokes a decode error as there must be 4 fields.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x13, (byte) 0xC0, // Described-type, ulong type, Flow descriptor, list8.
                0x03, 0x01, 0x43 }; // size (3), count (1), next-incoming-id (uint0).

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test
    public void testTruncatedFlowProvokesDecodeError2() {
        // Provide the bytes for Flow, but only give a next-incoming-id and incoming-window and next-outgoing-id. Provokes a decode error as there must be 4 fields.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x11, // Frame size = 17 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x13, (byte) 0xC0, // Described-type, ulong type, Flow descriptor, list8.
                0x05, 0x03, 0x43, 0x43, 0x43 }; // size (5), count (3), next-incoming-id (0), incoming-window (uint0), next-outgoing-id (uint0).

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "The outgoing-window field cannot be omitted");
    }

    @Test
    public void testTruncatedFlowProvokesDecodeError3() {
        // Provide the bytes for Flow, but only give a next-incoming-id and incoming-window and next-outgoing-id, and send not-present/null for outgoing-window. Provokes a decode error as must be present.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x12, // Frame size = 18 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x13, (byte) 0xC0, // Described-type, ulong type, Flow descriptor, list8.
                0x06, 0x04, 0x43, 0x43, 0x43, 0x40 }; // size (5), count (4), next-incoming-id (0), incoming-window (uint0), next-outgoing-id (uint0), outgoing-window (null).

        doInvalidFlowProvokesDecodeErrorTestImpl(bytes, "Unexpected null value - mandatory field not set? (the outgoing-window field is mandatory)");
    }

    private void doInvalidFlowProvokesDecodeErrorTestImpl(byte[] bytes, String description) {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        transport.bind(connection);
        connection.open();

        Session session = connection.session();
        session.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);

        // Provide the response bytes for the header
        transport.tail().put(AmqpHeader.HEADER);
        transport.process();


        // Send the necessary response to Open/Begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        int capacity = transport.capacity();
        assertTrue("Unexpected transport capacity: " + capacity, capacity > bytes.length);

        transport.tail().put(bytes);
        transport.process();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
        FrameBody frameBody = transport.writes.get(2);
        assertTrue("Unexpected frame type", frameBody instanceof Close);

        // Expect the close frame generated to contain a decode error condition referencing the missing container-id.
        ErrorCondition expectedCondition = new ErrorCondition();
        expectedCondition.setCondition(AmqpError.DECODE_ERROR);
        expectedCondition.setDescription(description);

        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());
    }

    @Test
    public void testEmptyTransferProvokesDecodeError() {
        // Provide the bytes for Transfer, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x14, 0x45};// Described-type, ulong type, Transfer descriptor, list0.

        doInvalidTransferProvokesDecodeErrorTestImpl(bytes, "The handle field cannot be omitted");
    }

    @Test
    public void testTruncatedTransferProvokesDecodeError() {
        // Provide the bytes for Transfer, but only give a null for the not-present handle. Provokes a decode error as there must be a handle.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x14, (byte) 0xC0, // Described-type, ulong type, Transfer descriptor, list8.
                0x03, 0x01, 0x40 }; // size (3), count (1), handle (null / not-present).

        doInvalidTransferProvokesDecodeErrorTestImpl(bytes, "Unexpected null value - mandatory field not set? (the handle field is mandatory)");
    }

    @Test
    public void testTransferWithWrongHandleTypeCodeProvokesDecodeError() {
        // Provide the bytes for Transfer, but give the wrong type code for a not-really-present handle. Provokes a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0F, // Frame size = 15 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x14, (byte) 0xC0, // Described-type, ulong type, Transfer descriptor, list8.
                0x03, 0x01, (byte) 0xA3 }; // size (3), count (1), handle (invalid sym8 type constructor given, not really present).

        doInvalidTransferProvokesDecodeErrorTestImpl(bytes, "Expected UnsignedInteger type but found encoding: SYM8:0xa3");
    }

    private void doInvalidTransferProvokesDecodeErrorTestImpl(byte[] bytes, String description) {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        transport.bind(connection);
        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "myReceiver";
        Receiver receiver = session.receiver(linkName);
        receiver.flow(5);
        receiver.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Flow);

        // Provide the response bytes for the header
        transport.tail().put(AmqpHeader.HEADER);
        transport.process();

        // Send the necessary response to Open/Begin/Attach
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.SENDER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        int capacity = transport.capacity();
        assertTrue("Unexpected transport capacity: " + capacity, capacity > bytes.length);

        transport.tail().put(bytes);
        transport.process();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        FrameBody frameBody = transport.writes.get(4);
        assertTrue("Unexpected frame type", frameBody instanceof Close);

        // Expect the close frame generated to contain a decode error condition referencing the missing container-id.
        ErrorCondition expectedCondition = new ErrorCondition();
        expectedCondition.setCondition(AmqpError.DECODE_ERROR);
        expectedCondition.setDescription(description);

        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());
    }

    @Test
    public void testEmptyDispositionProvokesDecodeError() {
        // Provide the bytes for Disposition, but omit any fields to provoke a decode error.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x0C, // Frame size = 12 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x15, 0x45};// Described-type, ulong type, Disposition descriptor, list0.

        doInvalidDispositionProvokesDecodeErrorTestImpl(bytes, "The 'first' field cannot be omitted");
    }

    @Test
    public void testTruncatedDispositionProvokesDecodeError() {
        // Provide the bytes for Disposition, but only give a null/not-present for the 'first' field. Provokes a decode error as there must be a role and 'first'.
        byte[] bytes = new byte[] {  0x00, 0x00, 0x00, 0x10, // Frame size = 16 bytes.
                0x02, 0x00, 0x00, 0x00, // DOFF, TYPE, 2x CHANNEL
                0x00, 0x53, 0x15, (byte) 0xC0, // Described-type, ulong type, Disposition descriptor, list8.
                0x04, 0x02, 0x41, 0x40 }; // size (4), count (2), role (receiver - the peers perspective), first ( null / not-present)

        doInvalidDispositionProvokesDecodeErrorTestImpl(bytes, "Unexpected null value - mandatory field not set? (the first field is mandatory)");
    }

    private void doInvalidDispositionProvokesDecodeErrorTestImpl(byte[] bytes, String description) {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        transport.bind(connection);
        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "mySender";
        Sender sender = session.sender(linkName);
        sender.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Provide the response bytes for the header
        transport.tail().put(AmqpHeader.HEADER);
        transport.process();

        // Send the necessary response to Open/Begin/Attach plus some credit
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        int credit = 1;
        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setDrain(true);
        flow.setLinkCredit(UnsignedInteger.valueOf(credit));
        transport.handleFrame(new TransportFrame(0, flow, null));

        sendMessage(sender, "tag1", "content1");

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Transfer);

        int capacity = transport.capacity();
        assertTrue("Unexpected transport capacity: " + capacity, capacity > bytes.length);

        transport.tail().put(bytes);
        transport.process();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        FrameBody frameBody = transport.writes.get(4);
        assertTrue("Unexpected frame type", frameBody instanceof Close);

        // Expect the close frame generated to contain a decode error condition referencing the missing container-id.
        ErrorCondition expectedCondition = new ErrorCondition();
        expectedCondition.setCondition(AmqpError.DECODE_ERROR);
        expectedCondition.setDescription(description);

        assertEquals("Unexpected condition", expectedCondition, ((Close) frameBody).getError());
    }

    private void prepareAndOpenConnection(MockTransportImpl transport, Connection connection) {
        transport.bind(connection);
        connection.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 1, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);

        // Give the necessary response to open
        transport.handleFrame(new TransportFrame(0, new Open(), null));
    }

    @Test
    public void testProtocolTracingLogsFrameToTracer()
    {
        Connection connection = new ConnectionImpl();
        List<TransportFrame> frames = new ArrayList<>();
        _transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void receivedFrame(final TransportFrame transportFrame)
            {
                frames.add(transportFrame);
            }

            @Override
            public void sentFrame(TransportFrame transportFrame) { }
        });

        assertTrue(_transport.isHandlingFrames());
        _transport.bind(connection);

        assertTrue(_transport.isHandlingFrames());
        _transport.handleFrame(TRANSPORT_FRAME_OPEN);
        assertTrue(_transport.isHandlingFrames());

        assertEquals(1, frames.size());
        TransportFrame transportFrame = frames.get(0);
        assertTrue(transportFrame.getBody() instanceof Open);
        assertEquals(CHANNEL_ID, transportFrame.getChannel());
    }

    @Test
    public void testProtocolTracingLogsFrameToSystem()
    {
        Connection connection = new ConnectionImpl();
        TransportImpl spy = spy(_transport);

        assertTrue(spy.isHandlingFrames());
        spy.bind(connection);

        assertTrue(spy.isHandlingFrames());
        spy.handleFrame(TRANSPORT_FRAME_OPEN);
        assertTrue(spy.isHandlingFrames());

        ArgumentCaptor<TransportFrame> frameCatcher = ArgumentCaptor.forClass(TransportFrame.class);
        Mockito.verify(spy).log(eq(TransportImpl.INCOMING), frameCatcher.capture());

        assertEquals(TRANSPORT_FRAME_OPEN.getChannel(), frameCatcher.getValue().getChannel());
        assertTrue(frameCatcher.getValue().getBody() instanceof Open);
        assertNull(frameCatcher.getValue().getPayload());
    }

    @Test
    public void testProtocolTracingLogsHeaderToTracer()
    {
        doProtocolTracingLogsHeaderToTracerTestImpl(false);
    }

    @Test
    public void testProtocolTracingLogsHeaderSaslToTracer()
    {
        doProtocolTracingLogsHeaderToTracerTestImpl(true);
    }

    private void doProtocolTracingLogsHeaderToTracerTestImpl(boolean sasl)
    {
        Connection connection = new ConnectionImpl();
        AtomicReference<String> headerRef = new AtomicReference<>();
        _transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void receivedHeader(String header)
            {
                assertTrue(headerRef.compareAndSet(null, header));
            }

            @Override
            public void receivedFrame(TransportFrame transportFrame) { }
            @Override
            public void sentFrame(TransportFrame transportFrame) { }

        });

        if (sasl)
        {
            _transport.sasl();
        }

        assertTrue(_transport.isHandlingFrames());
        _transport.bind(connection);

        assertTrue(_transport.isHandlingFrames());
        _transport.getInputBuffer().put(sasl ? AmqpHeader.SASL_HEADER : AmqpHeader.HEADER);
        _transport.process();
        assertTrue(_transport.isHandlingFrames());

        assertNotNull(headerRef.get());
        assertEquals(sasl ? "SASL" : "AMQP", headerRef.get());
    }

    @Test
    public void testProtocolTracingLogsHeaderToSystem()
    {
        doProtocolTracingLogsHeaderToSystemTestImpl(false);
    }

    @Test
    public void testProtocolTracingLogsHeaderSaslToSystem()
    {
        doProtocolTracingLogsHeaderToSystemTestImpl(true);
    }

    private void doProtocolTracingLogsHeaderToSystemTestImpl(boolean sasl)
    {
        Connection connection = new ConnectionImpl();

        AtomicReference<String> headerRef = new AtomicReference<>();
        AtomicReference<String> eventRef = new AtomicReference<>();
        TransportImpl transport = new TransportImpl()
        {
            @Override
            public void log(String event, String header) {
                assertTrue(eventRef.compareAndSet(null, event));
                assertTrue(headerRef.compareAndSet(null, header));
            }
        };
        transport.trace(2);

        if (sasl)
        {
            transport.sasl();
        }

        transport.bind(connection);

        transport.getInputBuffer().put(sasl ? AmqpHeader.SASL_HEADER : AmqpHeader.HEADER);
        transport.process();

        assertEquals(TransportImpl.INCOMING, eventRef.get());
        assertEquals(sasl ? "SASL" : "AMQP", headerRef.get());
    }

    @Test
    public void testProtocolTracingLogsOutboundHeaderToTracer()
    {
        doProtocolTracingLogsOutboundHeaderToTracerTestImpl(false);
    }

    @Test
    public void testProtocolTracingLogsOutboundHeaderSaslToTracer()
    {
        doProtocolTracingLogsOutboundHeaderToTracerTestImpl(true);
    }

    private void doProtocolTracingLogsOutboundHeaderToTracerTestImpl(boolean sasl)
    {
        Connection connection = new ConnectionImpl();
        AtomicReference<String> headerRef = new AtomicReference<>();
        _transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void sentHeader(String header)
            {
                assertTrue(headerRef.compareAndSet(null, header));
            }

            @Override
            public void receivedFrame(TransportFrame transportFrame) { }
            @Override
            public void sentFrame(TransportFrame transportFrame) { }

        });

        if (sasl)
        {
            _transport.sasl();
        }

        _transport.bind(connection);

        ByteBuffer expected = ByteBuffer.wrap(sasl ? AmqpHeader.SASL_HEADER : AmqpHeader.HEADER);

        _transport.pending();
        assertEquals(expected, _transport.getOutputBuffer());

        assertEquals(sasl ? "SASL" : "AMQP", headerRef.get());
    }

    @Test
    public void testProtocolTracingLogsOutboundHeaderToSystem()
    {
        doProtocolTracingLogsOutboundHeaderToSystemTestImpl(false);
    }

    @Test
    public void testProtocolTracingLogsOutboundHeaderSaslToSystem()
    {
        doProtocolTracingLogsOutboundHeaderToSystemTestImpl(true);
    }

    private void doProtocolTracingLogsOutboundHeaderToSystemTestImpl(boolean sasl)
    {
        Connection connection = new ConnectionImpl();

        AtomicReference<String> headerRef = new AtomicReference<>();
        AtomicReference<String> eventRef = new AtomicReference<>();
        TransportImpl transport = new TransportImpl()
        {
            @Override
            public void log(String event, String header)
            {
                assertTrue(eventRef.compareAndSet(null, event));
                assertTrue(headerRef.compareAndSet(null, header));
            }
        };
        transport.trace(2);

        if (sasl)
        {
            transport.sasl();
        }

        transport.bind(connection);

        ByteBuffer expected = ByteBuffer.wrap(sasl ? AmqpHeader.SASL_HEADER : AmqpHeader.HEADER);

        transport.pending();
        assertEquals(expected, transport.getOutputBuffer());

        assertEquals(TransportImpl.OUTGOING, eventRef.get());
        assertEquals(sasl ? "SASL" : "AMQP", headerRef.get());
    }

    /**
     * Verify that no Disposition frame is emitted by the Transport should a Delivery
     * have disposition applied after the delivery has been settled previously.
     */
    @Test
    public void testNoDispositionUpdatesAfterSettlementProceessedSender()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "myReceiver";
        Receiver receiver = session.receiver(linkName);
        receiver.flow(5);
        receiver.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Flow);

        Delivery delivery = receiver.current();
        assertNull("Should not yet have a delivery", delivery);

        // Send the necessary responses to open/begin/attach as well as a transfer
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.SENDER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        String deliveryTag = "tag1";
        String messageContent = "content1";
        handleTransfer(transport, 1, deliveryTag, messageContent);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        delivery = verifyDelivery(receiver, deliveryTag, messageContent);
        assertNotNull("Should now have a delivery", delivery);

        delivery.disposition(Accepted.getInstance());
        delivery.settle();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Disposition);

        // Should not produce any new frames being written
        delivery.disposition(Accepted.getInstance());

        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(5) instanceof Close);
    }

    /**
     * Verify that no Disposition frame is emitted by the Transport should a Delivery
     * have disposition applied after the delivery has been settled previously.
     */
    @Test
    public void testNoDispositionUpdatesAfterSettlementProceessedReceiver()
    {
        MockTransportImpl transport = new MockTransportImpl();
        Connection connection = Proton.connection();
        transport.bind(connection);

        connection.open();

        Session session = connection.session();
        session.open();

        String linkName = "myReceiver";
        Sender sender = session.sender(linkName);
        sender.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(0) instanceof Open);
        assertTrue("Unexpected frame type", transport.writes.get(1) instanceof Begin);
        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);

        // Send the necessary responses to open/begin/attach as well as a transfer
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        Attach attach = new Attach();
        attach.setHandle(UnsignedInteger.ZERO);
        attach.setRole(Role.RECEIVER);
        attach.setName(linkName);
        attach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, attach, null));

        int credit = 1;
        Flow flow = new Flow();
        flow.setHandle(UnsignedInteger.ZERO);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setDrain(true);
        flow.setLinkCredit(UnsignedInteger.valueOf(credit));
        transport.handleFrame(new TransportFrame(0, flow, null));

        Delivery delivery = sendMessage(sender, "tag1", "content1");

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Transfer);

        delivery.disposition(Accepted.getInstance());
        delivery.settle();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Disposition);

        // Should not produce any new frames being written
        delivery.disposition(Accepted.getInstance());

        connection.close();
        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(5) instanceof Close);
    }

    @Test
    public void testParallelOpenOfSenderAndReceiverLinksWithSameName()
    {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        connection.open();

        Session session = connection.session();
        session.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertEvents(collector, Event.Type.CONNECTION_INIT, Event.Type.CONNECTION_LOCAL_OPEN, Event.Type.TRANSPORT,
                Event.Type.SESSION_INIT, Event.Type.SESSION_LOCAL_OPEN, Event.Type.TRANSPORT);

        // Give the necessary responses to open/begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        final UnsignedInteger receiverHandle = UnsignedInteger.ZERO;
        final UnsignedInteger senderHandle = UnsignedInteger.ONE;

        // Open a receiver and a sender with same link name in parallel, i.e send both
        // attaches, such that both are locally open but neither is yet remotely open.
        String linkName = "myLinkName";
        Receiver receiver = session.receiver(linkName);
        assertEndpointState(receiver, EndpointState.UNINITIALIZED, EndpointState.UNINITIALIZED);

        Sender sender = session.sender(linkName);
        assertEndpointState(sender, EndpointState.UNINITIALIZED, EndpointState.UNINITIALIZED);

        receiver.open();
        sender.open();

        assertEndpointState(receiver, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);
        assertEndpointState(sender, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);

        assertEvents(collector, Event.Type.CONNECTION_REMOTE_OPEN, Event.Type.SESSION_REMOTE_OPEN,
                Event.Type.LINK_INIT, Event.Type.LINK_INIT, Event.Type.LINK_LOCAL_OPEN, Event.Type.TRANSPORT,
                Event.Type.LINK_LOCAL_OPEN, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        Attach receiverAttach = (Attach) transport.writes.get(2);
        validateAttach(receiverAttach, true, linkName, receiverHandle);

        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Attach);
        Attach senderAttach = (Attach) transport.writes.get(3);
        validateAttach(senderAttach, false, linkName, senderHandle);

        // Give the necessary response to attach for receiver
        Attach recieverResponseAttach = new Attach();
        recieverResponseAttach.setHandle(receiverHandle);
        recieverResponseAttach.setRole(Role.SENDER); // we receive, so peer response is sender role
        recieverResponseAttach.setName(linkName);
        recieverResponseAttach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, recieverResponseAttach, null));

        // Verify the local link state and events were updated as expected
        assertEndpointState(receiver, EndpointState.ACTIVE, EndpointState.ACTIVE);

        Event recieverRemoteOpenEvent = collector.peek();
        assertSame("Unexpected event context", receiver, recieverRemoteOpenEvent.getContext());
        assertEquals(Event.Type.LINK_REMOTE_OPEN, recieverRemoteOpenEvent.getEventType());
        assertEvents(collector, Event.Type.LINK_REMOTE_OPEN);

        // Give the necessary response to attach for sender
        Attach senderResponseAttach = new Attach();
        senderResponseAttach.setHandle(senderHandle);
        senderResponseAttach.setRole(Role.RECEIVER); // we send, so peer response is receiver role
        senderResponseAttach.setName(linkName);
        senderResponseAttach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, senderResponseAttach, null));

        // Verify the local link state and events were updated as expected

        assertEndpointState(sender, EndpointState.ACTIVE, EndpointState.ACTIVE);

        Event senderRemoteOpenEvent = collector.peek();
        assertSame("Unexpected event context", sender, senderRemoteOpenEvent.getContext());
        assertEquals(Event.Type.LINK_REMOTE_OPEN, senderRemoteOpenEvent.getEventType());
        assertEvents(collector, Event.Type.LINK_REMOTE_OPEN);

        // Use the opened sender and receiver a bit
        exerciseOpenedSenderAndReceiver(transport, collector, session, sender, senderHandle, receiver, receiverHandle);
    }

    @Test
    public void testSequentialOpenOfSenderAndReceiverLinksWithSameName() {
        MockTransportImpl transport = new MockTransportImpl();
        transport.setEmitFlowEventOnSend(false);
        Connection connection = Proton.connection();
        transport.bind(connection);

        Collector collector = Collector.Factory.create();
        connection.collect(collector);

        connection.open();

        Session session = connection.session();
        session.open();

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 2, transport.writes.size());

        assertEvents(collector, Event.Type.CONNECTION_INIT, Event.Type.CONNECTION_LOCAL_OPEN, Event.Type.TRANSPORT,
                Event.Type.SESSION_INIT, Event.Type.SESSION_LOCAL_OPEN, Event.Type.TRANSPORT);

        // Give the necessary responses to open/begin
        transport.handleFrame(new TransportFrame(0, new Open(), null));

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 0));
        begin.setNextOutgoingId(UnsignedInteger.ONE);
        begin.setIncomingWindow(UnsignedInteger.valueOf(1024));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        transport.handleFrame(new TransportFrame(0, begin, null));

        final UnsignedInteger receiverHandle = UnsignedInteger.ZERO;
        final UnsignedInteger senderHandle = UnsignedInteger.ONE;

        // Open a receiver
        String linkName = "myLinkName";
        Receiver receiver = session.receiver(linkName);
        assertEndpointState(receiver, EndpointState.UNINITIALIZED, EndpointState.UNINITIALIZED);
        receiver.open();

        assertEndpointState(receiver, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);

        assertEvents(collector, Event.Type.CONNECTION_REMOTE_OPEN, Event.Type.SESSION_REMOTE_OPEN,
                Event.Type.LINK_INIT, Event.Type.LINK_LOCAL_OPEN, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 3, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(2) instanceof Attach);
        Attach receiverAttach = (Attach) transport.writes.get(2);
        validateAttach(receiverAttach, true, linkName, receiverHandle);

        // Give the necessary response to attach for receiver
        Attach recieverResponseAttach = new Attach();
        recieverResponseAttach.setHandle(receiverHandle);
        recieverResponseAttach.setRole(Role.SENDER); // we receive, so peer response is sender role
        recieverResponseAttach.setName(linkName);
        recieverResponseAttach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, recieverResponseAttach, null));

        // Verify the local link state and events were updated as expected
        assertEndpointState(receiver, EndpointState.ACTIVE, EndpointState.ACTIVE);

        Event recieverRemoteOpenEvent = collector.peek();
        assertSame("Unexpected event context", receiver, recieverRemoteOpenEvent.getContext());
        assertEquals(Event.Type.LINK_REMOTE_OPEN, recieverRemoteOpenEvent.getEventType());
        assertEvents(collector, Event.Type.LINK_REMOTE_OPEN);

        // Open a sender, with same link name
        Sender sender = session.sender(linkName);
        assertEndpointState(sender, EndpointState.UNINITIALIZED, EndpointState.UNINITIALIZED);
        sender.open();

        assertEndpointState(sender, EndpointState.ACTIVE, EndpointState.UNINITIALIZED);

        assertEvents(collector, Event.Type.LINK_INIT, Event.Type.LINK_LOCAL_OPEN, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 4, transport.writes.size());

        assertTrue("Unexpected frame type", transport.writes.get(3) instanceof Attach);
        Attach senderAttach = (Attach) transport.writes.get(3);
        validateAttach(senderAttach, false, linkName, senderHandle);

        // Give the necessary response to attach for sender
        Attach senderResponseAttach = new Attach();
        senderResponseAttach.setHandle(senderHandle);
        senderResponseAttach.setRole(Role.RECEIVER); // we send, so peer response is receiver role
        senderResponseAttach.setName(linkName);
        senderResponseAttach.setInitialDeliveryCount(UnsignedInteger.ZERO);
        transport.handleFrame(new TransportFrame(0, senderResponseAttach, null));

        // Verify the local link state and events were updated as expected

        assertEndpointState(sender, EndpointState.ACTIVE, EndpointState.ACTIVE);

        Event senderRemoteOpenEvent = collector.peek();
        assertSame("Unexpected event context", sender, senderRemoteOpenEvent.getContext());
        assertEquals(Event.Type.LINK_REMOTE_OPEN, senderRemoteOpenEvent.getEventType());
        assertEvents(collector, Event.Type.LINK_REMOTE_OPEN);

        // Use the opened sender and receiver a bit
        exerciseOpenedSenderAndReceiver(transport, collector, session, sender, senderHandle, receiver, receiverHandle);
    }

    private void validateAttach(Attach attach, boolean isReceiver, String linkName, UnsignedInteger handle)
    {
        Role role = attach.getRole();
        assertEquals(isReceiver ? Role.RECEIVER : Role.SENDER, role);
        assertEquals(isReceiver, role.getValue());
        assertEquals(linkName, attach.getName());
        assertEquals(handle, attach.getHandle());
    }

    protected void assertEndpointState(Endpoint endpoint, EndpointState localState, EndpointState remoteState)
    {
        assertEquals(localState, endpoint.getLocalState());
        assertEquals(remoteState, endpoint.getRemoteState());
    }

    private void exerciseOpenedSenderAndReceiver(MockTransportImpl transport, Collector collector, Session session,
                                                 Sender sender, UnsignedInteger senderHandle, Receiver receiver, UnsignedInteger receiverHandle)
    {
        // Send a delivery
        Flow flow = new Flow();
        flow.setHandle(senderHandle);
        flow.setDeliveryCount(UnsignedInteger.ZERO);
        flow.setNextIncomingId(UnsignedInteger.ONE);
        flow.setNextOutgoingId(UnsignedInteger.ZERO);
        flow.setIncomingWindow(UnsignedInteger.valueOf(1024));
        flow.setOutgoingWindow(UnsignedInteger.valueOf(1024));
        flow.setLinkCredit(UnsignedInteger.valueOf(5));

        transport.handleFrame(new TransportFrame(0, flow, null));

        Event senderFlowEvent = collector.peek();
        assertEquals(Event.Type.LINK_FLOW, senderFlowEvent.getEventType());
        assertSame("Unexpected event context", sender, senderFlowEvent.getContext());
        assertEvents(collector, Event.Type.LINK_FLOW);

        String sentDeliveryTag = "sendTag";
        sendMessage(sender, sentDeliveryTag, "content");
        assertEvents(collector, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 5, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(4) instanceof Transfer);
        Transfer transfer = (Transfer) transport.writes.get(4);
        assertEquals(senderHandle, transfer.getHandle());
        assertEquals(UnsignedInteger.ZERO, transfer.getDeliveryId());
        assertEquals(new Binary(sentDeliveryTag.getBytes(StandardCharsets.UTF_8)), transfer.getDeliveryTag());
        assertEquals(false, transfer.getMore());

        assertNoEvents(collector);

        // Receive a delivery
        int credits = 10;
        receiver.flow(credits);
        assertEvents(collector, Event.Type.TRANSPORT);

        pumpMockTransport(transport);

        assertEquals("Unexpected frames written: " + getFrameTypesWritten(transport), 6, transport.writes.size());
        assertTrue("Unexpected frame type", transport.writes.get(5) instanceof Flow);
        Flow sentFlow = (Flow) transport.writes.get(5);
        assertEquals(receiverHandle, sentFlow.getHandle());
        assertEquals(UnsignedInteger.valueOf(credits), sentFlow.getLinkCredit());
        assertEquals(UnsignedInteger.ZERO, sentFlow.getDeliveryCount());

        assertNoEvents(collector);
        String recievedDeliveryTag = "recvTag";
        handleTransfer(transport, receiverHandle, 1, recievedDeliveryTag, new byte[]{ 1, 2, 3 });
        assertEvents(collector, Event.Type.DELIVERY);

        assertEquals("Unexpected queued count", 1, receiver.getQueued());
        assertEquals("Unexpected credit", 10, receiver.getCredit());
        assertEquals("Unexpected incoming bytes count", 3, session.getIncomingBytes());

        verifyDeliveryRawPayload(receiver, recievedDeliveryTag, new byte[] { 1, 2, 3 });

        assertEquals("Unexpected queued count", 0, receiver.getQueued());
        assertEquals("Unexpected credit", 9, receiver.getCredit());
        assertEquals("Unexpected incoming bytes count", 0, session.getIncomingBytes());

        assertNoEvents(collector);
    }
}
