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
import static org.apache.qpid.proton.engine.EndpointState.UNINITIALIZED;
import static org.apache.qpid.proton.systemtests.TestLoggingHelper.bold;
import static org.junit.Assert.assertEquals;

import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.junit.Test;

public class DeliveryTest extends EngineTestBase
{
    private static final Logger LOGGER = Logger.getLogger(DeliveryTest.class.getName());

    private final String _sourceAddress = getServer().containerId + "-link1-source";

    @Test
    public void testMessageFormat() throws Exception
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

        int messagCount = 4;
        getClient().receiver.flow(messagCount);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to create messages and send to the client"));

        sendMessageToClient("delivery1", "Msg1", null); // Don't set it, so it should be defaulted
        sendMessageToClient("delivery2", "Msg2", 0); // Explicitly set it to the default
        sendMessageToClient("delivery3", "Msg3", 1);
        sendMessageToClient("delivery4", "Msg4", UnsignedInteger.MAX_VALUE.intValue()); // Limit

        pumpServerToClient();

        LOGGER.fine(bold("======== About to process the messages on the client"));

        Delivery clientDelivery1 = receiveMessageFromServer("delivery1", "Msg1");
        Delivery clientDelivery2 = receiveMessageFromServer("delivery2", "Msg2");
        Delivery clientDelivery3 = receiveMessageFromServer("delivery3", "Msg3");
        Delivery clientDelivery4 = receiveMessageFromServer("delivery4", "Msg4");

        // Verify the message format is as expected
        assertEquals("Unexpected message format", 0, clientDelivery1.getMessageFormat());
        assertEquals("Unexpected message format", 0, clientDelivery2.getMessageFormat());
        assertEquals("Unexpected message format", 1, clientDelivery3.getMessageFormat());
        assertEquals("Unexpected message format", UnsignedInteger.MAX_VALUE.intValue(), clientDelivery4.getMessageFormat());
    }

    @Test
    public void testSendReceiveLargeMessage() throws Exception
    {
        doTestSendReceiveLargeMessage(-1, -1);
    }

    @Test
    public void testSendReceiveLargeMessageWithFrameSizeLimits() throws Exception
    {
        doTestSendReceiveLargeMessage(128 * 1024, 128 * 1024);
    }

    private void doTestSendReceiveLargeMessage(int maxFrameSize, int maxOutboundFrameSize) throws Exception
    {
        LOGGER.fine(bold("======== About to create transports"));

        getClient().transport = Proton.transport();
        if (maxFrameSize > 0) {
            getClient().transport.setMaxFrameSize(maxFrameSize);
        }
        if (maxOutboundFrameSize > 0) {
            getClient().transport.setOutboundFrameSizeLimit(maxOutboundFrameSize);
        }
        ProtocolTracerEnabler.setProtocolTracer(getClient().transport, TestLoggingHelper.CLIENT_PREFIX);

        getServer().transport = Proton.transport();
        if (maxFrameSize > 0) {
            getServer().transport.setMaxFrameSize(maxFrameSize);
        }
        if (maxOutboundFrameSize > 0) {
            getServer().transport.setOutboundFrameSizeLimit(maxOutboundFrameSize);
        }
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

        getClient().receiver.flow(1);

        pumpClientToServer();

        String messageContent = createMessageContent(256 * 1024);

        LOGGER.fine(bold("======== About to create messages and send to the client"));

        sendMessageToClient("delivery1", messageContent, 0); // Explicitly set it to the default

        pumpAllServerPendingToClient();

        LOGGER.fine(bold("======== About to process the messages on the client"));

        Delivery clientDelivery = receiveMessageFromServer("delivery1", messageContent);

        // Verify the message format is as expected
        assertEquals("Unexpected message format", 0, clientDelivery.getMessageFormat());
    }

}
