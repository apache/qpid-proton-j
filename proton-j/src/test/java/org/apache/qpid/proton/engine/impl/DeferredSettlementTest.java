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
package org.apache.qpid.proton.engine.impl;

import static java.util.EnumSet.of;
import static org.apache.qpid.proton.engine.EndpointState.ACTIVE;
import static org.apache.qpid.proton.engine.EndpointState.UNINITIALIZED;
import static org.apache.qpid.proton.systemtests.TestLoggingHelper.bold;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.systemtests.EngineTestBase;
import org.apache.qpid.proton.systemtests.ProtocolTracerEnabler;
import org.apache.qpid.proton.systemtests.TestLoggingHelper;
import org.junit.Test;

public class DeferredSettlementTest extends EngineTestBase
{
    private static final Logger LOGGER = Logger.getLogger(DeferredSettlementTest.class.getName());

    private static final int BUFFER_SIZE = 4096;

    private final String _sourceAddress = getServer().getContainerId() + "-link1-source";

    @Test
    public void testDeferredOutOfOrderSettlement() throws Exception
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
        assertEndpointState(serverSession, UNINITIALIZED, ACTIVE);

        serverSession.open();
        assertEndpointState(serverSession, ACTIVE, ACTIVE);

        pumpServerToClient();
        assertEndpointState(clientSession, ACTIVE, ACTIVE);

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

        assertEndpointState(clientReceiver, UNINITIALIZED, UNINITIALIZED);

        clientReceiver.open();
        assertEndpointState(clientReceiver, ACTIVE, UNINITIALIZED);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to set up implicitly created sender"));

        Sender serverSender = (Sender) getServer().getConnection().linkHead(of(UNINITIALIZED), of(ACTIVE));
        getServer().setSender(serverSender);

        serverSender.setReceiverSettleMode(serverSender.getRemoteReceiverSettleMode());
        serverSender.setSenderSettleMode(serverSender.getRemoteSenderSettleMode());

        org.apache.qpid.proton.amqp.transport.Source serverRemoteSource = serverSender.getRemoteSource();
        serverSender.setSource(serverRemoteSource);

        assertEndpointState(serverSender, UNINITIALIZED, ACTIVE);
        serverSender.open();

        assertEndpointState(serverSender, ACTIVE, ACTIVE);

        pumpServerToClient();

        assertEndpointState(clientReceiver, ACTIVE, ACTIVE);

        int messageCount = 5;
        clientReceiver.flow(messageCount);

        pumpClientToServer();

        LOGGER.fine(bold("======== About to create messages and send to the client"));

        DeliveryImpl[] serverDeliveries = sendMessagesToClient(messageCount);

        pumpServerToClient();

        for (int i = 0; i < messageCount; i++) {
            Delivery d = serverDeliveries[i];
            assertNotNull("Should have had a delivery", d);
            assertNull("Delivery shouldnt have local state", d.getLocalState());
            assertNull("Delivery shouldnt have remote state", d.getRemoteState());
        }

        LOGGER.fine(bold("======== About to process the messages on the client"));

        // Grab the original linkHead, assert deliveries are there, keep refs for later
        DeliveryImpl d0 = (DeliveryImpl) clientReceiver.head();
        assertNotNull("Should have a link head", d0);
        DeliveryImpl[] origClientLinkDeliveries = new DeliveryImpl[messageCount];
        for (int i = 0 ; i < messageCount; i++) {
            origClientLinkDeliveries[i] = d0;

            DeliveryImpl linkPrevious = d0.getLinkPrevious();
            DeliveryImpl linkNext = d0.getLinkNext();

            if(i == 0) {
                assertNull("should not have link prev", linkPrevious);
            } else {
                assertNotNull("should have link prev", linkPrevious);
                assertSame("Unexpected delivery at link prev", origClientLinkDeliveries[i - 1], linkPrevious);
                assertSame("Expected to be prior deliveries link next", d0, origClientLinkDeliveries[i - 1].getLinkNext());
            }

            if(i != messageCount - 1) {
                assertNotNull("should have link next", linkNext);
            } else {
                assertNull("should not have link next", linkNext);
            }

            d0 = linkNext;
        }

        // Receive the deliveries and verify contents, marking with matching integer context for easy identification.
        DeliveryImpl[] clientDeliveries = receiveMessagesFromServer(messageCount);

        // Accept but don't settle them all
        for (int i = 0; i < messageCount; i++) {
            Delivery d = clientDeliveries[i];
            assertNotNull("Should have had a delivery", d);

            d.disposition(Accepted.getInstance());
        }

        // Verify the client lists, i.e. deliveries now point to each other where expected
        for (int i = 0 ; i < messageCount; i++) {
            DeliveryImpl d = origClientLinkDeliveries[i];

            assertSame("Unexpected delivery", origClientLinkDeliveries[i], clientDeliveries[i]);

            // Verify the Transport and Link list entries
            if(i == 0) {
                assertDeliveryLinkReferences(d, i, null, origClientLinkDeliveries[1]);
                assertDeliveryTransportWorkReferences(d, i, null, origClientLinkDeliveries[1]);
            } else if (i != messageCount - 1) {
                assertDeliveryLinkReferences(d, i, origClientLinkDeliveries[i - 1], origClientLinkDeliveries[i+1]);
                assertDeliveryTransportWorkReferences(d, i, origClientLinkDeliveries[i - 1], origClientLinkDeliveries[i+1]);
            }
            else {
                assertDeliveryLinkReferences(d, i, origClientLinkDeliveries[i - 1], null);
                assertDeliveryTransportWorkReferences(d, i, origClientLinkDeliveries[i - 1], null);
            }

            // Assert there are no 'work' list entries, as those are for remote peer updates.
            assertDeliveryWorkReferences(d, i, null, null);
        }

        // Verify the server gets intended state changes
        pumpClientToServer();
        for (int i = 0; i < messageCount; i++) {
            DeliveryImpl d = serverDeliveries[i];
            assertNotNull("Should have had a delivery", d);
            assertNull("Delivery shouldnt have local state", d.getLocalState());
            assertEquals("Delivery should have remote state", Accepted.getInstance(), d.getRemoteState());

            // Verify the Link and Work list entries
            if(i == 0) {
                assertDeliveryLinkReferences(d, null, null, serverDeliveries[1]);
                assertDeliveryWorkReferences(d, null, null, serverDeliveries[1]);
            } else if (i != messageCount - 1) {
                assertDeliveryLinkReferences(d, null, serverDeliveries[i - 1], serverDeliveries[i+1]);
                assertDeliveryWorkReferences(d, null, serverDeliveries[i - 1], serverDeliveries[i+1]);
            }
            else {
                assertDeliveryLinkReferences(d, null, serverDeliveries[i - 1], null);
                assertDeliveryWorkReferences(d, null, serverDeliveries[i - 1], null);
            }

            // Assert there are no 'transport work' list entries, as those are for local updates.
            assertDeliveryTransportWorkReferences(d, null, null, null);
        }

        // Settle one from the middle
        int toSettle = messageCount/2;
        assertTrue("need more deliveries", toSettle > 1);
        assertTrue("need more deliveries", toSettle < messageCount - 1);

        DeliveryImpl dSettle = clientDeliveries[toSettle];
        Integer index = getDeliveryContextIndex(dSettle);

        // Verify the server gets intended state changes when settled
        assertFalse("Delivery should not have been remotely settled yet", serverDeliveries[toSettle].remotelySettled());
        dSettle.settle();

        // Verify the client delivery Link and Work list entries are cleared, tpWork is set
        assertDeliveryLinkReferences(dSettle, index, null, null);
        assertDeliveryWorkReferences(dSettle, index, null, null);
        assertDeliveryTransportWorkReferences(dSettle, index, null, null);
        assertSame("expected settled delivery to be client connection tpWork head", dSettle, ((ConnectionImpl) clientConnection).getTransportWorkHead());

        // Verify the client Link and Work list entries are correct for neighbouring deliveries
        assertDeliveryLinkReferences(clientDeliveries[toSettle - 1], index - 1, clientDeliveries[toSettle - 2], clientDeliveries[toSettle + 1]);
        assertDeliveryTransportWorkReferences(clientDeliveries[toSettle - 1], index - 1, null, null);
        assertDeliveryWorkReferences(clientDeliveries[toSettle - 1], index - 1, null, null);

        assertDeliveryLinkReferences(clientDeliveries[toSettle + 1], index + 1, clientDeliveries[toSettle - 1], clientDeliveries[toSettle + 2]);
        assertDeliveryTransportWorkReferences(clientDeliveries[toSettle + 1], index + 1, null, null);
        assertDeliveryWorkReferences(clientDeliveries[toSettle + 1], index + 1, null, null);

        // Update the server with the changes
        pumpClientToServer();

        // Verify server delivery is now remotelySettled, its Link and Work entries are NOT yet clear, but tpWork IS clear
        DeliveryImpl dSettleServer = serverDeliveries[toSettle];
        assertTrue("Delivery should have been remotely settled on server", dSettleServer.remotelySettled());

        assertDeliveryLinkReferences(dSettleServer, null, serverDeliveries[toSettle - 1], serverDeliveries[toSettle+1]);
        assertDeliveryWorkReferences(dSettleServer, null, serverDeliveries[toSettle - 1], serverDeliveries[toSettle+1]);
        assertDeliveryTransportWorkReferences(dSettleServer, null, null, null);

        assertNull("expected client connection tpWork head to now be null", ((ConnectionImpl) clientConnection).getTransportWorkHead());

        // Settle on server, expect Link and Work list entries to be cleared, tpWork to remain clear (as delivery
        // is already remotely settled). Note 'work next' returns list head if none present, so we verify that here.
        dSettleServer.settle();

        assertDeliveryLinkReferences(dSettleServer, null, null, null);
        assertNull("Unexpected workPrev", dSettleServer.getWorkPrev());
        assertSame("Unexpected workNext", serverDeliveries[0], dSettleServer.getWorkNext());
        assertDeliveryTransportWorkReferences(dSettleServer, null, null, null);

        assertNull("expected server connection tpWork head to still be null", ((ConnectionImpl) serverConnection).getTransportWorkHead());

        // Verify the server entries are correct for neighbouring deliveries updated to reflect the settle
        assertDeliveryLinkReferences(serverDeliveries[toSettle - 1], null, serverDeliveries[toSettle - 2], serverDeliveries[toSettle + 1]);
        assertDeliveryWorkReferences(serverDeliveries[toSettle - 1], null, serverDeliveries[toSettle - 2], serverDeliveries[toSettle + 1]);
        assertDeliveryTransportWorkReferences(serverDeliveries[toSettle - 1], null, null, null);

        assertDeliveryLinkReferences(serverDeliveries[toSettle + 1], null, serverDeliveries[toSettle - 1], serverDeliveries[toSettle + 2]);
        assertDeliveryWorkReferences(serverDeliveries[toSettle + 1], null, serverDeliveries[toSettle - 1], serverDeliveries[toSettle + 2]);
        assertDeliveryTransportWorkReferences(serverDeliveries[toSettle + 1], null, null, null);
    }

    private Integer getDeliveryContextIndex(DeliveryImpl d) {
        assertNotNull("Should have had a delivery", d);
        Integer index = (Integer) d.getContext();
        assertNotNull("Should have had a context index", index);

        return index;
    }

    private void assertDeliveryWorkReferences(DeliveryImpl delivery, Integer index, DeliveryImpl deliveryWorkPrev, DeliveryImpl deliveryWorkNext) {
        assertNotNull("No delivery given", delivery);
        if(index != null) {
            assertEquals("Unexpected context index", Integer.valueOf(index), getDeliveryContextIndex(delivery));
        }

        if(deliveryWorkPrev == null) {
            assertNull("Unexpected workPrev", delivery.getWorkPrev());
        } else {
            assertSame("Unexpected workPrev", deliveryWorkPrev, delivery.getWorkPrev());
            assertSame("Unexpected workNext on previous delivery", delivery, deliveryWorkPrev.getWorkNext());
        }

        if(deliveryWorkNext == null) {
            assertNull("Unexpected workNext", delivery.getWorkNext());
        } else {
            assertSame("Unexpected workNext", deliveryWorkNext, delivery.getWorkNext());
            assertSame("Unexpected workPrev on next delivery",  delivery , deliveryWorkNext.getWorkPrev());
        }
    }

    private void assertDeliveryTransportWorkReferences(DeliveryImpl delivery, Integer index, DeliveryImpl deliveryTpWorkPrev, DeliveryImpl deliveryTpWorkNext) {
        assertNotNull("No delivery given", delivery);
        if(index != null) {
            assertEquals("Unexpected context index", Integer.valueOf(index), getDeliveryContextIndex(delivery));
        }

        if(deliveryTpWorkPrev == null) {
            assertNull("Unexpected transportWorkPrev", delivery.getTransportWorkPrev());
        } else {
            assertSame("Unexpected transportWorkPrev", deliveryTpWorkPrev, delivery.getTransportWorkPrev());
            assertSame("Unexpected transportWorkNext on previous delivery", delivery, deliveryTpWorkPrev.getTransportWorkNext());
        }

        if (deliveryTpWorkNext == null) {
            assertNull("Unexpected transportWorkNext", delivery.getTransportWorkNext());
        } else {
            assertSame("Unexpected transportWorkNext", deliveryTpWorkNext, delivery.getTransportWorkNext());
            assertSame("Unexpected transportWorkPrev on next delivery",  delivery , deliveryTpWorkNext.getTransportWorkPrev());
        }
    }

    private void assertDeliveryLinkReferences(DeliveryImpl delivery, Integer index, DeliveryImpl deliveryLinkPrev, DeliveryImpl deliveryLinkNext) {
        assertNotNull("No delivery given", delivery);
        if(index != null) {
            assertEquals("Unexpected context index", Integer.valueOf(index), getDeliveryContextIndex(delivery));
        }

        if(deliveryLinkPrev == null) {
            assertNull("Unexpected linkPrev", delivery.getLinkPrevious());
        } else {
            assertSame("Unexpected linkPrev", deliveryLinkPrev, delivery.getLinkPrevious());
            assertSame("Unexpected linkPrev on previous delivery", delivery, deliveryLinkPrev.getLinkNext());
        }

        if(deliveryLinkNext == null) {
            assertNull("Unexpected linkNext", delivery.getLinkNext());
        } else {
            assertSame("Unexpected linkNext", deliveryLinkNext, delivery.getLinkNext());
            assertSame("Unexpected linkPrev on next delivery",  delivery , deliveryLinkNext.getLinkPrevious());
        }
    }

    private DeliveryImpl[] receiveMessagesFromServer(int count) {
        DeliveryImpl[] deliveries = new DeliveryImpl[count];
        for(int i = 0; i < count; i++) {
            deliveries[i] = (DeliveryImpl) receiveMessageFromServer("Message" + i, i);
        }

        return deliveries;
    }

    private Delivery receiveMessageFromServer(String deliveryTag, int count)
    {
        Delivery delivery = getClient().getConnection().getWorkHead();
        Receiver clientReceiver = getClient().getReceiver();

        assertTrue(Arrays.equals(deliveryTag.getBytes(StandardCharsets.UTF_8), delivery.getTag()));
        assertEquals("The received delivery should be on our receiver",
                clientReceiver, delivery.getLink());

        assertNull(delivery.getLocalState());
        assertNull(delivery.getRemoteState());

        assertFalse(delivery.isPartial());
        assertTrue(delivery.isReadable());

        int size = delivery.available();
        byte[] received = new byte[size];

        int len = clientReceiver.recv(received, 0, size);

        assertEquals("Should have received " + size + " bytes", size, len);
        assertEquals("Should be no bytes left", 0, delivery.available());

        Message m = Proton.message();
        m.decode(received, 0, len);

        Object messageBody = ((AmqpValue)m.getBody()).getValue();
        assertEquals("Unexpected message content", count, messageBody);

        boolean receiverAdvanced = clientReceiver.advance();
        assertTrue("receiver has not advanced", receiverAdvanced);

        delivery.setContext(count);

        return delivery;
    }

    private DeliveryImpl[] sendMessagesToClient(int count) {
        DeliveryImpl[] deliveries = new DeliveryImpl[count];
        for(int i = 0; i< count; i++) {
            deliveries[i] = (DeliveryImpl) sendMessageToClient("Message" + i, i);
        }

        return deliveries;
    }

    private Delivery sendMessageToClient(String deliveryTag, int messageBody)
    {
        byte[] tag = deliveryTag.getBytes(StandardCharsets.UTF_8);

        Message m = Proton.message();
        m.setBody(new AmqpValue(messageBody));

        byte[] encoded = new byte[BUFFER_SIZE];
        int len = m.encode(encoded, 0, BUFFER_SIZE);

        assertTrue("given array was too small", len < BUFFER_SIZE);

        Sender serverSender = getServer().getSender();
        Delivery serverDelivery = serverSender.delivery(tag);
        int sent = serverSender.send(encoded, 0, len);

        assertEquals("sender unable to send all data at once as assumed for simplicity", len, sent);

        boolean senderAdvanced = serverSender.advance();
        assertTrue("sender has not advanced", senderAdvanced);

        return serverDelivery;
    }
}
