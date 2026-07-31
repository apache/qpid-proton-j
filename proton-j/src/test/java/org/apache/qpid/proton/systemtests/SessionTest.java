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
 */
package org.apache.qpid.proton.systemtests;

import static java.util.EnumSet.of;
import static org.apache.qpid.proton.amqp.transport.LinkError.TRANSFER_LIMIT_EXCEEDED;
import static org.apache.qpid.proton.amqp.transport.SessionError.WINDOW_VIOLATION;
import static org.apache.qpid.proton.engine.EndpointState.ACTIVE;
import static org.apache.qpid.proton.engine.EndpointState.CLOSED;
import static org.apache.qpid.proton.engine.EndpointState.UNINITIALIZED;
import static org.apache.qpid.proton.systemtests.TestLoggingHelper.bold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Collector;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportException;
import org.apache.qpid.proton.engine.impl.AmqpFramer;
import org.apache.qpid.proton.engine.impl.LinkImpl;
import org.apache.qpid.proton.engine.impl.SessionImpl;
import org.apache.qpid.proton.engine.impl.TransportLinkTestAccessor;
import org.apache.qpid.proton.engine.impl.TransportSessionTestAccessor;
import org.junit.Test;

public class SessionTest extends EngineTestBase
{
    private static final Logger LOGGER = Logger.getLogger(SessionTest.class.getName());

    @Test
    public void testCapabilities() throws Exception
    {
        final Symbol clientOfferedCap = Symbol.valueOf("clientOfferedCapability");
        final Symbol clientDesiredCap = Symbol.valueOf("clientDesiredCapability");
        final Symbol serverOfferedCap = Symbol.valueOf("serverOfferedCapability");
        final Symbol serverDesiredCap = Symbol.valueOf("serverDesiredCapability");

        Symbol[] clientOfferedCapabilities = new Symbol[] { clientOfferedCap };
        Symbol[] clientDesiredCapabilities = new Symbol[] { clientDesiredCap };

        Symbol[] serverOfferedCapabilities = new Symbol[] { serverOfferedCap };
        Symbol[] serverDesiredCapabilities = new Symbol[] { serverDesiredCap };

        LOGGER.fine(bold("======== About to create transports"));

        getClient().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getClient().transport, TestLoggingHelper.CLIENT_PREFIX);

        getServer().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getServer().transport, "            " + TestLoggingHelper.SERVER_PREFIX);

        doOutputInputCycle();

        getClient().connection = Proton.connection();
        getClient().transport.bind(getClient().connection);

        getServer().connection = Proton.connection();
        getServer().transport.bind(getServer().connection);

        LOGGER.fine(bold("======== About to open connections"));
        getClient().connection.open();
        getServer().connection.open();

        doOutputInputCycle();

        LOGGER.fine(bold("======== About to open sessions"));
        getClient().session = getClient().connection.session();

        // Set the client session capabilities
        getClient().session.setOfferedCapabilities(clientOfferedCapabilities);
        getClient().session.setDesiredCapabilities(clientDesiredCapabilities);

        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        // Set the server session capabilities
        getServer().session.setOfferedCapabilities(serverOfferedCapabilities);
        getServer().session.setDesiredCapabilities(serverDesiredCapabilities);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();

        // Verify server side got the clients session capabilities as expected
        Symbol[] serverRemoteOfferedCapabilities = getServer().session.getRemoteOfferedCapabilities();
        assertNotNull("Server had no remote offered capabilities", serverRemoteOfferedCapabilities);
        assertEquals("Server remote offered capabilities not expected size", 1, serverRemoteOfferedCapabilities.length);
        assertTrue("Server remote offered capabilities lack expected value: " + clientOfferedCap, Arrays.asList(serverRemoteOfferedCapabilities).contains(clientOfferedCap));

        Symbol[] serverRemoteDesiredCapabilities = getServer().session.getRemoteDesiredCapabilities();
        assertNotNull("Server had no remote desired capabilities", serverRemoteDesiredCapabilities);
        assertEquals("Server remote desired capabilities not expected size", 1, serverRemoteDesiredCapabilities.length);
        assertTrue("Server remote desired capabilities lack expected value: " + clientDesiredCap, Arrays.asList(serverRemoteDesiredCapabilities).contains(clientDesiredCap));

        // Verify the client side got the servers session capabilities as expected
        Symbol[]  clientRemoteOfferedCapabilities = getClient().session.getRemoteOfferedCapabilities();
        assertNotNull("Client had no remote offered capabilities", clientRemoteOfferedCapabilities);
        assertEquals("Client remote offered capabilities not expected size", 1, clientRemoteOfferedCapabilities.length);
        assertTrue("Client remote offered capabilities lack expected value: " + serverOfferedCap, Arrays.asList(clientRemoteOfferedCapabilities).contains(serverOfferedCap));

        Symbol[]  clientRemoteDesiredCapabilities = getClient().session.getRemoteDesiredCapabilities();
        assertNotNull("Client had no remote desired capabilities", clientRemoteDesiredCapabilities);
        assertEquals("Client remote desired capabilities not expected size", 1, clientRemoteDesiredCapabilities.length);
        assertTrue("Client remote desired capabilities lack expected value: " + serverDesiredCap, Arrays.asList(clientRemoteDesiredCapabilities).contains(serverDesiredCap));
    }

    @Test
    public void testProperties() throws Exception
    {
        final Symbol clientPropName = Symbol.valueOf("ClientPropName");
        final Integer clientPropValue = 1234;
        final Symbol serverPropName = Symbol.valueOf("ServerPropName");
        final Integer serverPropValue = 5678;

        Map<Symbol, Object> clientProps = new HashMap<>();
        clientProps.put(clientPropName, clientPropValue);

        Map<Symbol, Object> serverProps = new HashMap<>();
        serverProps.put(serverPropName, serverPropValue);

        LOGGER.fine(bold("======== About to create transports"));

        getClient().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getClient().transport, TestLoggingHelper.CLIENT_PREFIX);

        getServer().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getServer().transport, "            " + TestLoggingHelper.SERVER_PREFIX);

        doOutputInputCycle();

        getClient().connection = Proton.connection();
        getClient().transport.bind(getClient().connection);

        getServer().connection = Proton.connection();
        getServer().transport.bind(getServer().connection);

        LOGGER.fine(bold("======== About to open connections"));
        getClient().connection.open();
        getServer().connection.open();

        doOutputInputCycle();

        LOGGER.fine(bold("======== About to open sessions"));
        getClient().session = getClient().connection.session();

        // Set the client session properties
        getClient().session.setProperties(clientProps);

        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        // Set the server session properties
        getServer().session.setProperties(serverProps);

        getServer().session.open();

        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        // Verify server side got the clients session properties as expected
        Map<Symbol, Object> serverRemoteProperties = getServer().session.getRemoteProperties();
        assertNotNull("Server had no remote properties", serverRemoteProperties);
        assertEquals("Server remote properties not expected size", 1, serverRemoteProperties.size());
        assertTrue("Server remote properties lack expected key: " + clientPropName, serverRemoteProperties.containsKey(clientPropName));
        assertEquals("Server remote properties contain unexpected value for key: " + clientPropName, clientPropValue, serverRemoteProperties.get(clientPropName));

        // Verify the client side got the servers session properties as expected
        Map<Symbol, Object> clientRemoteProperties = getClient().session.getRemoteProperties();
        assertNotNull("Client had no remote properties", clientRemoteProperties);
        assertEquals("Client remote properties not expected size", 1, clientRemoteProperties.size());
        assertTrue("Client remote properties lack expected key: " + serverPropName, clientRemoteProperties.containsKey(serverPropName));
        assertEquals("Client remote properties contain unexpected value for key: " + serverPropName, serverPropValue, clientRemoteProperties.get(serverPropName));
    }

    @Test
    public void testBlownSessionIncomingWindow() throws Exception
    {
        LOGGER.fine(bold("======== About to create transports"));

        getClient().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getClient().transport, TestLoggingHelper.CLIENT_PREFIX);

        getServer().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getServer().transport, "            " + TestLoggingHelper.SERVER_PREFIX);

        getClient().connection = Proton.connection();
        getClient().transport.bind(getClient().connection);

        getServer().connection = Proton.connection();
        getServer().transport.bind(getServer().connection);
        getServer().transport.setMaxFrameSize(5 * 1024);
        Collector collector = Collector.Factory.create();
        getServer().connection.collect(collector);

        LOGGER.fine(bold("======== About to open connections"));
        getClient().connection.open();
        getServer().connection.open();

        doOutputInputCycle();

        LOGGER.fine(bold("======== About to open sessions"));
        getClient().session = getClient().connection.session();
        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        getServer().session.setIncomingCapacity(100 * 1024);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();

        UnsignedInteger window = TransportSessionTestAccessor.getTransportSessionIncomingWindow((SessionImpl) getServer().session);
        assertNotNull(window);
        int sessionWindow = window.intValue();
        assertEquals(20, sessionWindow);

        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create receiver"));

        getClient().source = new Source();

        getClient().target = new Target();
        final String targetAddress = "testBlownSessionIncomingWindow";
        getClient().target.setAddress(targetAddress);

        getClient().sender = getClient().session.sender("link1");
        getClient().sender.setSource(getClient().source);
        getClient().sender.setTarget(getClient().target);

        getClient().sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        getClient().sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        assertEndpointState(getClient().sender, UNINITIALIZED, UNINITIALIZED);

        getClient().sender.open();
        assertEndpointState(getClient().sender, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created receiver"));

        getServer().receiver = (Receiver) getServer().connection.linkHead(of(UNINITIALIZED), of(ACTIVE));

        getServer().receiver.setReceiverSettleMode(getServer().receiver.getRemoteReceiverSettleMode());
        getServer().receiver.setSenderSettleMode(getServer().receiver.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Target serverRemoteTarget = getServer().receiver.getRemoteTarget();
        getServer().receiver.setTarget(serverRemoteTarget);

        assertEndpointState(getServer().receiver, UNINITIALIZED, ACTIVE);
        getServer().receiver.open();

        assertEndpointState(getServer().receiver, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to flow credit"));

        int flowCount = 100;
        getServer().receiver.flow(flowCount);

        pumpServerToClient();

        assertEndpointState(getClient().sender, ACTIVE, ACTIVE);

        UnsignedInteger remoteHandle = TransportLinkTestAccessor.getTransportLinkRemoteHandle((LinkImpl) getClient().getSender());

        LOGGER.fine(bold("======== About to create messages and send to the server"));

        // 'Send' and verify credit state
        for (int i = 0; i < sessionWindow; i++) {
            sendMessageToServer("delivery" + i, "MsgContent" + i);
        }
        assertLinkCreditState(getClient().sender, flowCount - sessionWindow, sessionWindow, flowCount - sessionWindow);
        assertLinkCreditState(getServer().receiver, flowCount, 0, flowCount);

        // Pump to the server to send messages 'on the wire', verify new state
        pumpClientToServer();

        assertLinkCreditState(getClient().sender, flowCount - sessionWindow, 0, flowCount - sessionWindow);
        assertLinkCreditState(getServer().receiver, flowCount, sessionWindow, flowCount - sessionWindow);

        window = TransportSessionTestAccessor.getTransportSessionIncomingWindow((SessionImpl) getServer().session);
        assertNotNull(window);
        assertEquals(0, window.intValue());

        while(collector.peek() != null) {
            collector.pop();
        }

        LOGGER.fine(bold("======== About to prepare and send crafted delivery to the server to blow the window"));

        UnsignedInteger nextOutgoingId = TransportSessionTestAccessor.getTransportSessionNextOutgoingId((SessionImpl) getServer().session);
        assertNotNull(nextOutgoingId);

        // Send a Transfer to the server 'on behalf of the client'
        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x14); // Transfer ulong descriptor
        tmpBuf.put((byte) (byte) 0xD0); // list32 primitive type constructor
        tmpBuf.putInt(20); // size
        tmpBuf.putInt(4); // count
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(remoteHandle.intValue()); // handle
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(nextOutgoingId.intValue()); // delivery-id
        tmpBuf.put((byte) 0xA0); // vbin8 (delivery-tag)
        tmpBuf.put((byte) 3); // length
        tmpBuf.put((byte) 0x03); // delivery-tag
        tmpBuf.put((byte) 0x02);
        tmpBuf.put((byte) 0x01);
        tmpBuf.put((byte) 0x43); // message-format, uint0

        tmpBuf.put((byte) 0x06); // payload bytes (not part of performative above)
        tmpBuf.put((byte) 0x05);
        tmpBuf.put((byte) 0x04);

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateFrameRawBody(0, frameBodyBytes);

        final ByteBuffer clientToServerBuffer = ByteBuffer.wrap(frameBytes);

        // Send the bytes for the frame with Transfer performative
        pumpBufferFromClientToServer(clientToServerBuffer, false);

        assertLinkCreditState(getServer().receiver, flowCount, sessionWindow, flowCount - sessionWindow);

        final ErrorCondition condition = getServer().transport.getCondition();
        assertNotNull(condition);
        assertNotNull(condition.getCondition());
        assertEquals(WINDOW_VIOLATION, condition.getCondition());

        Event peek = collector.peek();
        assertNotNull(peek);
        assertEquals(Event.Type.TRANSPORT_ERROR, peek.getType());

        assertTrue(getServer().transport.pending() > 0);

        pumpServerToClient();

        assertEquals(CLOSED, getClient().connection.getRemoteState());
        ErrorCondition clientCondition = getClient().connection.getRemoteCondition();
        assertNotNull(clientCondition);
        assertNotNull(clientCondition.getCondition());
        assertEquals(WINDOW_VIOLATION, clientCondition.getCondition());

        assertEquals(Transport.END_OF_STREAM, getServer().transport.pending());
        assertEquals(Transport.END_OF_STREAM, getServer().transport.capacity());
        assertFalse(getServer().transport.head().hasRemaining());
        assertTrue(getServer().transport.isClosed());

        try
        {
            getServer().transport.tail();
            fail("Expected exception");
        }
        catch (TransportException te)
        {
            // Expected
        }
    }

    @Test
    public void testMaxTransfersPerDeliveryExceeded() throws Exception
    {
        doExceedMaxTransfersPerDeliveryTestImpl(2, 1, true);
    }

    @Test
    public void testMaxTransfersPerDeliveryEqualled() throws Exception
    {
        doExceedMaxTransfersPerDeliveryTestImpl(2, 2, false);
    }

    @Test
    public void testMaxTransfersPerDeliveryEqualledSingleFrame() throws Exception
    {
        doExceedMaxTransfersPerDeliveryTestImpl(1, 1, false);
    }

    @Test
    public void testMaxTransfersPerDeliveryNotHit() throws Exception
    {
        doExceedMaxTransfersPerDeliveryTestImpl(1, 2, false);
    }

    @Test
    public void testMaxTransfersPerDeliveryDisabled() throws Exception
    {
        doExceedMaxTransfersPerDeliveryTestImpl(3, 0, false);
    }

    private void doExceedMaxTransfersPerDeliveryTestImpl(int deliveryFrameCount, int maxTransfersPerDelivery, boolean expectFailure) throws Exception
    {
        final int outboundFrameSizeLimit = 512;
        assertTrue(deliveryFrameCount > 0);

        LOGGER.fine(bold("======== About to create transports"));

        getClient().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getClient().transport, TestLoggingHelper.CLIENT_PREFIX);

        getServer().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getServer().transport, "            " + TestLoggingHelper.SERVER_PREFIX);

        getClient().connection = Proton.connection();
        getClient().transport.bind(getClient().connection);

        getServer().connection = Proton.connection();
        getServer().transport.bind(getServer().connection);
        getServer().transport.setMaxFrameSize(5 * 1024);
        getClient().transport.setMaxFrameSize(5 * 1024);
        getClient().transport.setOutboundFrameSizeLimit(outboundFrameSizeLimit);

        getServer().transport.setMaxTransfersPerDelivery(maxTransfersPerDelivery);

        Collector collector = Collector.Factory.create();
        getServer().connection.collect(collector);

        LOGGER.fine(bold("======== About to open connections"));
        getClient().connection.open();
        getServer().connection.open();

        doOutputInputCycle();

        LOGGER.fine(bold("======== About to open sessions"));
        getClient().session = getClient().connection.session();
        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        getServer().session.setIncomingCapacity(100 * 1024);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create receiver"));

        getClient().source = new Source();

        getClient().target = new Target();
        final String targetAddress = "testBlownSessionIncomingWindow";
        getClient().target.setAddress(targetAddress);

        getClient().sender = getClient().session.sender("link1");
        getClient().sender.setSource(getClient().source);
        getClient().sender.setTarget(getClient().target);

        getClient().sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        getClient().sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        assertEndpointState(getClient().sender, UNINITIALIZED, UNINITIALIZED);

        getClient().sender.open();
        assertEndpointState(getClient().sender, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created receiver"));

        getServer().receiver = (Receiver) getServer().connection.linkHead(of(UNINITIALIZED), of(ACTIVE));

        getServer().receiver.setReceiverSettleMode(getServer().receiver.getRemoteReceiverSettleMode());
        getServer().receiver.setSenderSettleMode(getServer().receiver.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Target serverRemoteTarget = getServer().receiver.getRemoteTarget();
        getServer().receiver.setTarget(serverRemoteTarget);

        assertEndpointState(getServer().receiver, UNINITIALIZED, ACTIVE);
        getServer().receiver.open();

        assertEndpointState(getServer().receiver, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to flow credit"));

        int flowCount = 100;
        getServer().receiver.flow(flowCount);

        pumpServerToClient();

        assertEndpointState(getClient().sender, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create message and send to the server"));

        // Small amount for 1st frame, then add maxOutgoingFrameSize for provoking each extra frame
        // (Diff from small-amount to maxOutgoingFrameSize allowing for overhead of transfers, for several frames)
        int msgContentLength = 10 + (outboundFrameSizeLimit * (deliveryFrameCount - 1));
        String msgContent = createMessageContent(msgContentLength);

        // 'Send' and verify state
        sendMessageToServer("delivery", msgContent);

        assertLinkCreditState(getClient().sender, flowCount - 1, 1, flowCount - 1);
        assertLinkCreditState(getServer().receiver, flowCount, 0, flowCount);

        while(collector.peek() != null) {
            collector.pop();
        }

        // Pump to the server to send messages 'on the wire', verify new state
        pumpClientToServer();

        assertLinkCreditState(getClient().sender, flowCount - 1, 0, flowCount - 1);
        if (!expectFailure) {
            assertLinkCreditState(getServer().receiver, flowCount, 1, flowCount - 1);
        }

        if (expectFailure) {
            final ErrorCondition condition = getServer().transport.getCondition();
            assertNotNull(condition);
            assertNotNull(condition.getCondition());
            assertEquals(TRANSFER_LIMIT_EXCEEDED, condition.getCondition());

            if (deliveryFrameCount > 1) {
                Event peek = collector.peek();
                assertNotNull(peek);
                assertEquals(Event.Type.DELIVERY, peek.getType());
                collector.pop();
            }

            Event peek = collector.peek();
            assertNotNull(peek);
            assertEquals(Event.Type.TRANSPORT_ERROR, peek.getType());

            assertTrue(getServer().transport.pending() > 0);

            pumpServerToClient();

            assertEquals(CLOSED, getClient().connection.getRemoteState());
            ErrorCondition clientCondition = getClient().connection.getRemoteCondition();
            assertNotNull(clientCondition);
            assertNotNull(clientCondition.getCondition());
            assertEquals(TRANSFER_LIMIT_EXCEEDED, clientCondition.getCondition());

            assertEquals(Transport.END_OF_STREAM, getServer().transport.pending());
            assertEquals(Transport.END_OF_STREAM, getServer().transport.capacity());
            assertFalse(getServer().transport.head().hasRemaining());
            assertTrue(getServer().transport.isClosed());

            try
            {
                getServer().transport.tail();
                fail("Expected exception");
            }
            catch (TransportException te)
            {
                // Expected
            }
        } else {
            assertEquals(0, getServer().transport.pending());

            receiveMessageFromClient("delivery", msgContent);
        }
    }
}