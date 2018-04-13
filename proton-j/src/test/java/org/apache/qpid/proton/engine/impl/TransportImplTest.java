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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.Close;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.amqp.transport.End;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.apache.qpid.proton.amqp.transport.FrameBody;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.Role;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
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
            writes.addLast(frameBody);
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
}
