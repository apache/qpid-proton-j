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
import static org.apache.qpid.proton.engine.EndpointState.ACTIVE;
import static org.apache.qpid.proton.engine.EndpointState.CLOSED;
import static org.apache.qpid.proton.engine.EndpointState.UNINITIALIZED;
import static org.apache.qpid.proton.systemtests.TestLoggingHelper.bold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportException;
import org.apache.qpid.proton.engine.impl.AmqpFramer;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.engine.impl.TransportDelivery;
import org.apache.qpid.proton.engine.impl.TransportDeliveryTestAccessor;
import org.junit.Test;

public class LinkTest extends EngineTestBase
{
    private static final Logger LOGGER = Logger.getLogger(LinkTest.class.getName());

    private static final Symbol RCV_PROP = Symbol.valueOf("ReceiverPropName");
    private static final Integer RCV_PROP_VAL = 1234;
    private static final Symbol SND_PROP = Symbol.valueOf("SenderPropName");
    private static final Integer SND_PROP_VAL = 5678;
    private static final UnsignedLong CLIENT_MAX_MSG_SIZE = UnsignedLong.valueOf(54321);
    private static final UnsignedLong SERVER_MAX_MSG_SIZE = UnsignedLong.valueOf(12345);

    private final String _sourceAddress = getServer().containerId + "-link1-source";

    @Test
    public void testCapabilities() throws Exception
    {
        final Symbol recvOfferedCap = Symbol.valueOf("recvOfferedCapability");
        final Symbol recvDesiredCap = Symbol.valueOf("recvDesiredCapability");
        final Symbol senderOfferedCap = Symbol.valueOf("senderOfferedCapability");
        final Symbol senderDesiredCap = Symbol.valueOf("senderDesiredCapability");

        Symbol[] clientOfferedCapabilities = new Symbol[] { recvOfferedCap };
        Symbol[] clientDesiredCapabilities = new Symbol[] { recvDesiredCap };

        Symbol[] serverOfferedCapabilities = new Symbol[] { senderOfferedCap };
        Symbol[] serverDesiredCapabilities = new Symbol[] { senderDesiredCap };

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
        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();
        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create reciever"));

        getClient().source = new Source();
        getClient().source.setAddress(_sourceAddress);

        getClient().target = new Target();
        getClient().target.setAddress(null);

        getClient().receiver = getClient().session.receiver("link1");
        getClient().receiver.setTarget(getClient().target);
        getClient().receiver.setSource(getClient().source);

        getClient().receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        getClient().receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        // Set the client receivers capabilities
        getClient().receiver.setOfferedCapabilities(clientOfferedCapabilities);
        getClient().receiver.setDesiredCapabilities(clientDesiredCapabilities);

        assertEndpointState(getClient().receiver, UNINITIALIZED, UNINITIALIZED);

        getClient().receiver.open();
        assertEndpointState(getClient().receiver, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created sender"));

        getServer().sender = (Sender) getServer().connection.linkHead(of(UNINITIALIZED), of(ACTIVE));

        getServer().sender.setReceiverSettleMode(getServer().sender.getRemoteReceiverSettleMode());
        getServer().sender.setSenderSettleMode(getServer().sender.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Source serverRemoteSource = getServer().sender.getRemoteSource();
        getServer().sender.setSource(serverRemoteSource);

        // Set the server senders capabilities
        getServer().sender.setOfferedCapabilities(serverOfferedCapabilities);
        getServer().sender.setDesiredCapabilities(serverDesiredCapabilities);

        assertEndpointState(getServer().sender, UNINITIALIZED, ACTIVE);
        getServer().sender.open();

        assertEndpointState(getServer().sender, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(getClient().receiver, ACTIVE, ACTIVE);

        // Verify server side got the clients receiver capabilities as expected
        Symbol[] serverRemoteOfferedCapabilities = getServer().sender.getRemoteOfferedCapabilities();
        assertNotNull("Server had no remote offered capabilities", serverRemoteOfferedCapabilities);
        assertEquals("Server remote offered capabilities not expected size", 1, serverRemoteOfferedCapabilities.length);
        assertTrue("Server remote offered capabilities lack expected value: " + recvOfferedCap, Arrays.asList(serverRemoteOfferedCapabilities).contains(recvOfferedCap));

        Symbol[] serverRemoteDesiredCapabilities = getServer().sender.getRemoteDesiredCapabilities();
        assertNotNull("Server had no remote desired capabilities", serverRemoteDesiredCapabilities);
        assertEquals("Server remote desired capabilities not expected size", 1, serverRemoteDesiredCapabilities.length);
        assertTrue("Server remote desired capabilities lack expected value: " + recvDesiredCap, Arrays.asList(serverRemoteDesiredCapabilities).contains(recvDesiredCap));

        // Verify the client side got the servers sender capabilities as expected
        Symbol[]  clientRemoteOfferedCapabilities = getClient().receiver.getRemoteOfferedCapabilities();
        assertNotNull("Client had no remote offered capabilities", clientRemoteOfferedCapabilities);
        assertEquals("Client remote offered capabilities not expected size", 1, clientRemoteOfferedCapabilities.length);
        assertTrue("Client remote offered capabilities lack expected value: " + senderOfferedCap, Arrays.asList(clientRemoteOfferedCapabilities).contains(senderOfferedCap));

        Symbol[]  clientRemoteDesiredCapabilities = getClient().receiver.getRemoteDesiredCapabilities();
        assertNotNull("Client had no remote desired capabilities", clientRemoteDesiredCapabilities);
        assertEquals("Client remote desired capabilities not expected size", 1, clientRemoteDesiredCapabilities.length);
        assertTrue("Client remote desired capabilities lack expected value: " + senderDesiredCap, Arrays.asList(clientRemoteDesiredCapabilities).contains(senderDesiredCap));
    }

    @Test
    public void testProperties() throws Exception
    {
        Map<Symbol, Object> receiverProps = new HashMap<>();
        receiverProps.put(RCV_PROP, RCV_PROP_VAL);

        Map<Symbol, Object> senderProps = new HashMap<>();
        senderProps.put(SND_PROP, SND_PROP_VAL);

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
        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();
        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create reciever"));

        getClient().source = new Source();
        getClient().source.setAddress(_sourceAddress);

        getClient().target = new Target();
        getClient().target.setAddress(null);

        getClient().receiver = getClient().session.receiver("link1");
        getClient().receiver.setTarget(getClient().target);
        getClient().receiver.setSource(getClient().source);

        getClient().receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        getClient().receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        // Set the recievers properties
        getClient().receiver.setProperties(receiverProps);

        assertEndpointState(getClient().receiver, UNINITIALIZED, UNINITIALIZED);

        getClient().receiver.open();
        assertEndpointState(getClient().receiver, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created sender"));

        getServer().sender = (Sender) getServer().connection.linkHead(of(UNINITIALIZED), of(ACTIVE));

        getServer().sender.setReceiverSettleMode(getServer().sender.getRemoteReceiverSettleMode());
        getServer().sender.setSenderSettleMode(getServer().sender.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Source serverRemoteSource = getServer().sender.getRemoteSource();
        getServer().sender.setSource(serverRemoteSource);

        // Set the senders properties
        getServer().sender.setProperties(senderProps);

        assertEndpointState(getServer().sender, UNINITIALIZED, ACTIVE);
        getServer().sender.open();

        assertEndpointState(getServer().sender, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(getClient().receiver, ACTIVE, ACTIVE);

        // Verify server side got the clients receiver properties as expected
        Map<Symbol, Object> serverRemoteProperties = getServer().sender.getRemoteProperties();
        assertNotNull("Server had no remote properties", serverRemoteProperties);
        assertEquals("Server remote properties not expected size", 1, serverRemoteProperties.size());
        assertTrue("Server remote properties lack expected key: " + RCV_PROP, serverRemoteProperties.containsKey(RCV_PROP));
        assertEquals("Server remote properties contain unexpected value for key: " + RCV_PROP, RCV_PROP_VAL, serverRemoteProperties.get(RCV_PROP));

        // Verify the client side got the servers sender properties as expected
        Map<Symbol, Object> clientRemoteProperties = getClient().receiver.getRemoteProperties();
        assertNotNull("Client had no remote properties", clientRemoteProperties);
        assertEquals("Client remote properties not expected size", 1, clientRemoteProperties.size());
        assertTrue("Client remote properties lack expected key: " + SND_PROP, clientRemoteProperties.containsKey(SND_PROP));
        assertEquals("Client remote properties contain unexpected value for key: " + SND_PROP, SND_PROP_VAL, clientRemoteProperties.get(SND_PROP));
    }

    /**
     * Test the {@link Link#getCredit()}, {@link Link#getQueued()}, and
     * {@link Link#getRemoteCredit()} methods behave as expected when sending
     * from server and receiving on client links.
     *
     * @throws Exception
     *             if something unexpected occurs
     */
    @Test
    public void testLinkCreditState() throws Exception
    {
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
        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();
        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create reciever"));

        getClient().source = new Source();
        getClient().source.setAddress(_sourceAddress);

        getClient().target = new Target();
        getClient().target.setAddress(null);

        getClient().receiver = getClient().session.receiver("link1");
        getClient().receiver.setTarget(getClient().target);
        getClient().receiver.setSource(getClient().source);

        getClient().receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        getClient().receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        assertEndpointState(getClient().receiver, UNINITIALIZED, UNINITIALIZED);

        getClient().receiver.open();
        assertEndpointState(getClient().receiver, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created sender"));

        getServer().sender = (Sender) getServer().connection.linkHead(of(UNINITIALIZED), of(ACTIVE));

        getServer().sender.setReceiverSettleMode(getServer().sender.getRemoteReceiverSettleMode());
        getServer().sender.setSenderSettleMode(getServer().sender.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Source serverRemoteSource = getServer().sender.getRemoteSource();
        getServer().sender.setSource(serverRemoteSource);

        assertEndpointState(getServer().sender, UNINITIALIZED, ACTIVE);
        getServer().sender.open();

        assertEndpointState(getServer().sender, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(getClient().receiver, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to flow credit"));

        // Verify credit state
        assertLinkCreditState(getServer().sender, 0, 0, 0);
        assertLinkCreditState(getClient().receiver, 0, 0, 0);

        int messagCount = 4;
        getClient().receiver.flow(messagCount);

        // Verify credit state
        assertLinkCreditState(getServer().sender, 0, 0, 0);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        pumpClientToServer();

        // Verify credit state
        assertLinkCreditState(getServer().sender, 4, 0, 4);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        LOGGER.fine(bold("======== About to create messages and send to the client"));

        // 'Send' and verify credit state
        sendMessageToClient("delivery1", "Msg1");
        assertLinkCreditState(getServer().sender, 3, 1, 3);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        // 'Send' and verify credit state
        sendMessageToClient("delivery2", "Msg2");
        assertLinkCreditState(getServer().sender, 2, 2, 2);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        // Pump to the client to send messages 'on the wire', verify new state, process messages
        pumpServerToClient();

        LOGGER.fine(bold("======== About to process the messages on the client"));

        assertLinkCreditState(getServer().sender, 2, 0, 2);
        assertLinkCreditState(getClient().receiver, 4, 2, 2);

        receiveMessageFromServer("delivery1", "Msg1");

        assertLinkCreditState(getServer().sender, 2, 0, 2);
        assertLinkCreditState(getClient().receiver, 3, 1, 2);

        receiveMessageFromServer("delivery2", "Msg2");

        assertLinkCreditState(getServer().sender, 2, 0, 2);
        assertLinkCreditState(getClient().receiver, 2, 0, 2);
    }

    //TODO
    @Test
    public void testMaxMessageSizeValue() throws Exception
    {
        LOGGER.fine(bold("======== About to create transports"));

        Transport clientTransport = Proton.transport();
        getClient().setTransport(clientTransport);
        ProtocolTracerEnabler.setProtocolTracer(clientTransport, TestLoggingHelper.CLIENT_PREFIX);

        Transport serverTransport = Proton.transport();
        getServer().setTransport(serverTransport);
        ProtocolTracerEnabler.setProtocolTracer(serverTransport, "            " + TestLoggingHelper.SERVER_PREFIX);

        doOutputInputCycle();

        Connection clientConnection = Proton.connection();
        getClient().setConnection(clientConnection);
        clientTransport.bind(clientConnection);

        Connection serverConnection = Proton.connection();
        getServer().setConnection(serverConnection);
        serverTransport.bind(serverConnection);

        LOGGER.fine(bold("======== About to open connections"));
        clientConnection.open();
        serverConnection.open();

        doOutputInputCycle();

        LOGGER.fine(bold("======== About to open sessions"));
        Session clientSession = clientConnection.session();
        getClient().setSession(clientSession);
        clientSession.open();

        pumpClientToServer();

        Session serverSession = serverConnection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        getServer().setSession(serverSession);

        serverSession.open();

        pumpServerToClient();

        LOGGER.fine(bold("======== About to create receiver"));

        Source clientSource = new Source();
        getClient().setSource(clientSource);
        clientSource.setAddress(_sourceAddress);

        Target clientTarget = new Target();
        getClient().setTarget(clientTarget);
        clientTarget.setAddress(null);

        Receiver clientReceiver = clientSession.receiver("link1");
        getClient().setReceiver(clientReceiver);
        clientReceiver.setTarget(clientTarget);
        clientReceiver.setSource(clientSource);

        clientReceiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        clientReceiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        // Set the local link max-message-size
        assertNull("Expected no value to be set", clientReceiver.getMaxMessageSize());
        clientReceiver.setMaxMessageSize(CLIENT_MAX_MSG_SIZE);
        assertEquals("Expected value to be set", CLIENT_MAX_MSG_SIZE, clientReceiver.getMaxMessageSize());

        clientReceiver.open();
        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created sender"));

        Sender serverSender = (Sender) getServer().getConnection().linkHead(of(UNINITIALIZED), of(ACTIVE));
        getServer().setSender(serverSender);

        serverSender.setReceiverSettleMode(serverSender.getRemoteReceiverSettleMode());
        serverSender.setSenderSettleMode(serverSender.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Source serverRemoteSource = serverSender.getRemoteSource();
        serverSender.setSource(serverRemoteSource);

        assertEquals("Expected value to be set", CLIENT_MAX_MSG_SIZE, serverSender.getRemoteMaxMessageSize());

        // Set the local link max-message-size
        assertNull("Expected no value to be set", serverSender.getMaxMessageSize());
        serverSender.setMaxMessageSize(SERVER_MAX_MSG_SIZE);
        assertEquals("Expected value to be set", SERVER_MAX_MSG_SIZE, serverSender.getMaxMessageSize());

        serverSender.open();

        assertNull("Expected no value to be present yet", clientReceiver.getRemoteMaxMessageSize());

        pumpServerToClient();

        assertEquals("Expected value to be set", SERVER_MAX_MSG_SIZE, clientReceiver.getRemoteMaxMessageSize());
    }

    @Test(timeout=20000)
    public void testDispositionIllegalRangeFromZero() throws Exception
    {
        doDispositionIllegalRangeTestImpl(UnsignedInteger.ZERO, UnsignedInteger.valueOf(Integer.MAX_VALUE).add(UnsignedInteger.ONE));
    }

    @Test(timeout=20000)
    public void testDispositionIllegalRangeFromIntMaxValue() throws Exception
    {
        doDispositionIllegalRangeTestImpl(UnsignedInteger.valueOf(Integer.MAX_VALUE), UnsignedInteger.MAX_VALUE);
    }

    @Test(timeout=20000)
    public void testDispositionIllegalRangeSpanningIntMaxValue() throws Exception
    {
        doDispositionIllegalRangeTestImpl(UnsignedInteger.valueOf(100_000), UnsignedInteger.valueOf(Integer.MAX_VALUE + 100_000 + 1));
    }

    @Test(timeout=20000)
    public void testDispositionIllegalRangeSpanningMaxValue() throws Exception
    {
        doDispositionIllegalRangeTestImpl(UnsignedInteger.MAX_VALUE, UnsignedInteger.valueOf(Integer.MAX_VALUE));
    }

    private void doDispositionIllegalRangeTestImpl(final UnsignedInteger firstId, final UnsignedInteger lastId) throws Exception
    {
        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertFalse(delivery1.remotelySettled());
                assertNull(delivery1.getRemoteState());

                assertFalse(delivery2.remotelySettled());
                assertNull(delivery2.getRemoteState());

                assertFalse(delivery3.remotelySettled());
                assertNull(delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId.intValue(), lastId.intValue(), new AssertDeliveryNoState(), assertAfter, false);

        ErrorCondition serverCondition = getServer().transport.getCondition();
        assertNotNull("Expected server transport error condition object", serverCondition);
        assertNotNull("Expected server transport error condition", serverCondition.getCondition());
        assertEquals(AmqpError.NOT_ALLOWED, serverCondition.getCondition());
        assertEquals("Invalid disposition sequence range: " + firstId.longValue() + " to " + lastId.longValue(), serverCondition.getDescription());

        pumpServerToClient();

        ErrorCondition clientCondition = getClient().connection.getRemoteCondition();
        assertNotNull(clientCondition);
        assertNotNull(clientCondition.getCondition());
        assertEquals(AmqpError.NOT_ALLOWED, clientCondition.getCondition());

        assertEquals(CLOSED, getClient().connection.getRemoteState());

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

    @Test(timeout=20000)
    public void testDispositionLargeRangeAfter() throws Exception
    {
        final int firstId = 0;
        final int lastId = 1_000_000_000;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionLargeRangePartialAfter() throws Exception
    {
        final int firstId = 1;
        final int lastId = 1_000_000_000;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertFalse(delivery1.remotelySettled());
                assertNull(delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionLargeRangeBefore() throws Exception
    {
        final int firstId = UnsignedInteger.ZERO.subtract(UnsignedInteger.valueOf(1_000_000_000)).intValue();
        final int lastId = 2;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionLargeRangePartialBefore() throws Exception
    {
        final int firstId = 0 - 1_000_000_000;
        final int lastId = 1;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertFalse(delivery3.remotelySettled());
                assertNull(delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionLargeRangeSpanning() throws Exception
    {
        final int firstId = UnsignedInteger.MAX_VALUE.subtract(UnsignedInteger.valueOf(100)).intValue();
        final int lastId = 1_000_000_000;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionPartRangeStart() throws Exception
    {
        final int firstId = 0;
        final int lastId = 1;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertFalse(delivery3.remotelySettled());
                assertNull(delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionPartRangeStartWithWrap() throws Exception
    {
        final int firstId = UnsignedInteger.MAX_VALUE.intValue();
        final int lastId = 1;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertFalse(delivery3.remotelySettled());
                assertNull(delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionPartRangeEnd() throws Exception
    {
        final int firstId = 1;
        final int lastId = 2;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertFalse(delivery1.remotelySettled());
                assertNull(delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionPartRangePastEnd() throws Exception
    {
        final int firstId = 1;
        final int lastId = 10;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertFalse(delivery1.remotelySettled());
                assertNull(delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, false);
    }

    @Test(timeout=20000)
    public void testDispositionSingleAtStart() throws Exception
    {
        doDispositionSingleAtStartTestImpl(true);
    }

    @Test(timeout=20000)
    public void testDispositionSingleAtStartOmitLast() throws Exception
    {
        doDispositionSingleAtStartTestImpl(false);
    }

    private void doDispositionSingleAtStartTestImpl(boolean setLastIfSame) throws Exception
    {
        final int firstId = 0;
        final int lastId = 0;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertTrue(delivery1.remotelySettled());
                assertSame(Accepted.getInstance(), delivery1.getRemoteState());

                assertFalse(delivery2.remotelySettled());
                assertNull(delivery2.getRemoteState());

                assertFalse(delivery3.remotelySettled());
                assertNull(delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, setLastIfSame);
    }

    @Test(timeout=20000)
    public void testDispositionSingleInMiddle() throws Exception
    {
        doDispositionSingleInMiddleTestImpl(true);
    }

    @Test(timeout=20000)
    public void testDispositionSingleInMiddleOmitLast() throws Exception
    {
        doDispositionSingleInMiddleTestImpl(false);
    }

    private void doDispositionSingleInMiddleTestImpl(boolean setLastIfSame) throws Exception
    {
        final int firstId = 1;
        final int lastId = 1;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertFalse(delivery1.remotelySettled());
                assertNull(delivery1.getRemoteState());

                assertTrue(delivery2.remotelySettled());
                assertSame(Accepted.getInstance(), delivery2.getRemoteState());

                assertFalse(delivery3.remotelySettled());
                assertNull(delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, setLastIfSame);
    }

    @Test(timeout=20000)
    public void testDispositionSingleAtEnd() throws Exception
    {
        doDispositionSingleAtEndTestImpl(true);
    }

    @Test(timeout=20000)
    public void testDispositionSingleAtEndOmitLast() throws Exception
    {
        doDispositionSingleAtEndTestImpl(false);
    }

    private void doDispositionSingleAtEndTestImpl(boolean setLastIfSame) throws Exception
    {
        final int firstId = 2;
        final int lastId = 2;

        AssertDeliveryState assertAfter = new AssertDeliveryState()
        {
            @Override
            public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
            {
                assertFalse(delivery1.remotelySettled());
                assertNull(delivery1.getRemoteState());

                assertFalse(delivery2.remotelySettled());
                assertNull(delivery2.getRemoteState());

                assertTrue(delivery3.remotelySettled());
                assertSame(Accepted.getInstance(), delivery3.getRemoteState());
            }
        };

        doDispositionTestImpl(firstId, lastId, new AssertDeliveryNoState(), assertAfter, setLastIfSame);
    }

    private void doDispositionTestImpl(final int firstId, final int lastId, final AssertDeliveryState assertBefore, final AssertDeliveryState assertAfter, boolean setLastIfSame) throws Exception
    {
        boolean setLast = true;
        if (firstId == lastId) {
            setLast = setLastIfSame;
        }

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
        getClient().session.open();

        pumpClientToServer();

        getServer().session = getServer().connection.sessionHead(of(UNINITIALIZED), of(ACTIVE));
        assertEndpointState(getServer().session, UNINITIALIZED, ACTIVE);

        getServer().session.open();
        assertEndpointState(getServer().session, ACTIVE, ACTIVE);

        pumpServerToClient();
        assertEndpointState(getClient().session, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to create receiver"));

        getClient().source = new Source();
        getClient().source.setAddress(_sourceAddress);

        getClient().target = new Target();
        getClient().target.setAddress(null);

        getClient().receiver = getClient().session.receiver("link1");
        getClient().receiver.setTarget(getClient().target);
        getClient().receiver.setSource(getClient().source);

        getClient().receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
        getClient().receiver.setSenderSettleMode(SenderSettleMode.UNSETTLED);

        assertEndpointState(getClient().receiver, UNINITIALIZED, UNINITIALIZED);

        getClient().receiver.open();
        assertEndpointState(getClient().receiver, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created sender"));

        getServer().sender = (Sender) getServer().connection.linkHead(of(UNINITIALIZED), of(ACTIVE));

        getServer().sender.setReceiverSettleMode(getServer().sender.getRemoteReceiverSettleMode());
        getServer().sender.setSenderSettleMode(getServer().sender.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Source serverRemoteSource = getServer().sender.getRemoteSource();
        getServer().sender.setSource(serverRemoteSource);

        assertEndpointState(getServer().sender, UNINITIALIZED, ACTIVE);
        getServer().sender.open();

        assertEndpointState(getServer().sender, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(getClient().receiver, ACTIVE, ACTIVE);

        LOGGER.fine(bold("======== About to flow credit"));

        int flowCount = 4;
        getClient().receiver.flow(flowCount);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to create messages and send to the client"));

        // 'Send' and verify credit state
        Delivery serverDelivery1 = sendMessageToClient("delivery1", "Msg1");
        assertLinkCreditState(getServer().sender, 3, 1, 3);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        // 'Send' and verify credit state
        Delivery serverDelivery2 = sendMessageToClient("delivery2", "Msg2");
        assertLinkCreditState(getServer().sender, 2, 2, 2);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        // 'Send' and verify credit state
        Delivery serverDelivery3 = sendMessageToClient("delivery3", "Msg3");
        assertLinkCreditState(getServer().sender, 1, 3, 1);
        assertLinkCreditState(getClient().receiver, 4, 0, 4);

        // Pump to the client to send messages 'on the wire', verify new state, process messages
        pumpServerToClient();

        LOGGER.fine(bold("======== About to process the messages on the client"));

        Delivery clientDelivery1 =  receiveMessageFromServer("delivery1", "Msg1");

        TransportDelivery clientTransportDelivery1 = TransportDeliveryTestAccessor.getTransportDelivery((DeliveryImpl) clientDelivery1);
        assertEquals(UnsignedInteger.valueOf(0), clientTransportDelivery1.getDeliveryId());

        assertLinkCreditState(getServer().sender, 1, 0, 1);
        assertLinkCreditState(getClient().receiver, 3, 2, 1);

        Delivery clientDelivery2 =  receiveMessageFromServer("delivery2", "Msg2");

        TransportDelivery clientTransportDelivery2 = TransportDeliveryTestAccessor.getTransportDelivery((DeliveryImpl) clientDelivery2);
        assertEquals(UnsignedInteger.valueOf(1), clientTransportDelivery2.getDeliveryId());

        assertLinkCreditState(getServer().sender, 1, 0, 1);
        assertLinkCreditState(getClient().receiver, 2, 1, 1);

        Delivery clientDelivery3 =  receiveMessageFromServer("delivery3", "Msg3");

        TransportDelivery clientTransportDelivery3 = TransportDeliveryTestAccessor.getTransportDelivery((DeliveryImpl) clientDelivery3);
        assertEquals(UnsignedInteger.valueOf(2), clientTransportDelivery3.getDeliveryId());

        assertLinkCreditState(getServer().sender, 1, 0, 1);
        assertLinkCreditState(getClient().receiver, 1, 0, 1);

        LOGGER.fine(bold("======== About to prepare and send crafted disposition to the server transport"));

        // Send a Disposition to the server 'on behalf of the client' for the deliveries
        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x15); // Disposition ulong descriptor
        tmpBuf.put((byte) (byte) 0xC0); // list8 primitive type constructor
        tmpBuf.put((byte) (setLast ? 17 : 13)); // size
        tmpBuf.put((byte) 5); // count
        tmpBuf.put((byte) 0x41); // role (=true=receiver)
        tmpBuf.put((byte) 0x70); // uint
        tmpBuf.putInt(firstId); // first
        if (setLast)
        {
            tmpBuf.put((byte) 0x70); // uint
            tmpBuf.putInt(lastId); // last
        }
        else
        {
            tmpBuf.put((byte) 0x40); // null to omit last
        }
        tmpBuf.put((byte) 0x41); // settled = true
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put((byte) 0x53); // small-ulong
        tmpBuf.put((byte) 0x24); // Accepted
        tmpBuf.put((byte) 0x45); // list0 primitive type constructor

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateFrameRawBody(0, frameBodyBytes);

        final ByteBuffer clientToServerBuffer = ByteBuffer.wrap(frameBytes);

        assertBefore.assertDeliveryState(serverDelivery1, serverDelivery2, serverDelivery3);
        long start = System.nanoTime();

        // Send the bytes for the frame with disposition performative
        pumpBufferFromClientToServer(clientToServerBuffer, false);

        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue("Unexpected duration: " + duration, duration < 100);

        assertAfter.assertDeliveryState(serverDelivery1, serverDelivery2, serverDelivery3);
    }

    private final class AssertDeliveryNoState implements AssertDeliveryState
    {
        @Override
        public void assertDeliveryState(final Delivery delivery1, final Delivery delivery2, final Delivery delivery3)
        {
            assertFalse(delivery1.remotelySettled());
            assertNull(delivery1.getRemoteState());

            assertFalse(delivery2.remotelySettled());
            assertNull(delivery2.getRemoteState());

            assertFalse(delivery3.remotelySettled());
            assertNull(delivery3.getRemoteState());
        }
    }

    private interface AssertDeliveryState
    {
        void assertDeliveryState(Delivery delivery1, Delivery delivery2, Delivery delivery3);
    }
}