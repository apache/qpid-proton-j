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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;

public abstract class EngineTestBase
{
    private static final Logger LOGGER = Logger.getLogger(EngineTestBase.class.getName());

    private final TestLoggingHelper _testLoggingHelper = new TestLoggingHelper(LOGGER);
    private final ProtonContainer _client = new ProtonContainer("clientContainer");
    private final ProtonContainer _server = new ProtonContainer("serverContainer");

    protected boolean shouldLogPumpedBytes()
    {
        return true;
    }

    protected TestLoggingHelper getTestLoggingHelper()
    {
        return _testLoggingHelper;
    }

    protected ProtonContainer getClient()
    {
        return _client;
    }

    protected ProtonContainer getServer()
    {
        return _server;
    }

    protected void assertClientHasNothingToOutput()
    {
        assertEquals(0, getClient().transport.getOutputBuffer().remaining());
        getClient().transport.outputConsumed();
    }

    protected void pumpServerToClient()
    {
        ByteBuffer serverBuffer = getServer().transport.getOutputBuffer();

        pumpBufferFromServerToClient(serverBuffer, true);
    }

    protected void pumpBufferFromServerToClient(ByteBuffer serverBuffer, boolean markServerTransportOutputConsumed)
    {
        if (shouldLogPumpedBytes())
        {
            getTestLoggingHelper().prettyPrint("          <<<" + TestLoggingHelper.SERVER_PREFIX + " ", serverBuffer);
        }
        assertTrue("Server expected to produce some output", serverBuffer.hasRemaining());

        ByteBuffer clientBuffer = getClient().transport.getInputBuffer();

        clientBuffer.put(serverBuffer);

        assertEquals("Client expected to consume all server's output", 0, serverBuffer.remaining());

        getClient().transport.processInput().checkIsOk();

        if (markServerTransportOutputConsumed)
        {
            getServer().transport.outputConsumed();
        }
    }

    protected void pumpAllServerPendingToClient()
    {
        while (getServer().transport.pending() != 0)
        {
            ByteBuffer serverBuffer = getServer().transport.getOutputBuffer();

            if (shouldLogPumpedBytes())
            {
                getTestLoggingHelper().prettyPrint("          <<<" + TestLoggingHelper.SERVER_PREFIX + " ", serverBuffer);
            }
            assertTrue("Server expected to produce some output", serverBuffer.hasRemaining());

            ByteBuffer clientBuffer = getClient().transport.getInputBuffer();

            clientBuffer.put(serverBuffer);

            assertEquals("Client expected to consume all server's output", 0, serverBuffer.remaining());

            getClient().transport.processInput().checkIsOk();
            getServer().transport.outputConsumed();
        }

        assertEquals("Client expected to consume all server's pending work", 0, getServer().transport.pending());
    }

    protected void pumpClientToServer()
    {
        ByteBuffer clientBuffer = getClient().transport.getOutputBuffer();

        pumpBufferFromClientToServer(clientBuffer, true);
    }

    protected void pumpBufferFromClientToServer(ByteBuffer clientBuffer, boolean markClientTransportOutputConsumed) {
        if (shouldLogPumpedBytes())
        {
            getTestLoggingHelper().prettyPrint(TestLoggingHelper.CLIENT_PREFIX + ">>> ", clientBuffer);
        }
        assertTrue("Client expected to produce some output", clientBuffer.hasRemaining());

        ByteBuffer serverBuffer = getServer().transport.getInputBuffer();

        serverBuffer.put(clientBuffer);

        assertEquals("Server expected to consume all client's output", 0, clientBuffer.remaining());

        if(markClientTransportOutputConsumed) {
            getClient().transport.outputConsumed();
        }
        getServer().transport.processInput().checkIsOk();
    }

    protected void doOutputInputCycle() throws Exception
    {
        pumpClientToServer();

        pumpServerToClient();
    }

    protected String createMessageContent(int length)
    {
        Random rand = new Random(System.currentTimeMillis());

        byte[] payload = new byte[length];
        for (int i = 0; i < length; i++)
        {
            payload[i] = (byte) (64 + 1 + rand.nextInt(9));
        }

        return new String(payload, StandardCharsets.UTF_8);
    }

    protected void assertEndpointState(Endpoint endpoint, EndpointState localState, EndpointState remoteState)
    {
        assertEquals(localState, endpoint.getLocalState());
        assertEquals(remoteState, endpoint.getRemoteState());
    }

    protected void assertTerminusEquals(org.apache.qpid.proton.amqp.transport.Target expectedTarget, org.apache.qpid.proton.amqp.transport.Target actualTarget)
    {
        assertEquals(
                ((Target)expectedTarget).getAddress(),
                ((Target)actualTarget).getAddress());
    }

    protected void assertLinkCreditState(Link link, int credit, int queued, int remoteCredit)
    {
        assertEquals("Unexpected credit", credit, link.getCredit());
        assertEquals("Unexpected queued", queued, link.getQueued());
        assertEquals("Unexpected remote credit", remoteCredit, link.getRemoteCredit());
    }

    protected Delivery sendMessageToClient(String deliveryTag, String messageContent)
    {
        Sender sender = getServer().sender;
        assertNotNull("Server sender not initialised", sender);

        return sendMessage(sender, deliveryTag, messageContent, null);
    }

    protected Delivery sendMessageToClient(String deliveryTag, String messageContent, Integer messageFormat)
    {
        Sender sender = getServer().sender;
        assertNotNull("Server sender not initialised", sender);

        return sendMessage(sender, deliveryTag, messageContent, messageFormat);
    }

    protected Delivery sendMessageToServer(String deliveryTag, String messageContent)
    {
        Sender sender = getClient().sender;
        assertNotNull("Client sender not initialised", sender);

        return sendMessage(sender, deliveryTag, messageContent, null);
    }

    protected Delivery sendMessage(Sender sender, String deliveryTag, String messageContent, Integer messageFormat)
    {
        byte[] tag = deliveryTag.getBytes(StandardCharsets.UTF_8);

        Message m = Proton.message();
        m.setBody(new AmqpValue(messageContent));

        final int BUFFER_SIZE = messageContent.length() * 4;
        byte[] encoded = new byte[BUFFER_SIZE];
        int len = m.encode(encoded, 0, BUFFER_SIZE);

        assertTrue("given array was too small", len < BUFFER_SIZE);

        Delivery delivery = sender.delivery(tag);

        // Verify the default format of 0 is in place
        assertEquals("Unexpected message format", 0, delivery.getMessageFormat());
        // Set the message format explicitly if given, or leave it at the default
        if(messageFormat != null) {
            delivery.setMessageFormat(messageFormat);
        }

        int sent = sender.send(encoded, 0, len);

        assertEquals("sender unable to send all data at once as assumed for simplicity", len, sent);

        boolean senderAdvanced = sender.advance();
        assertTrue("sender has not advanced", senderAdvanced);

        return delivery;
    }

    protected Delivery receiveMessageFromServer(String deliveryTag, String messageContent)
    {
        final Receiver receiver = getClient().receiver;
        final Delivery workHead = getClient().connection.getWorkHead();

        return receiveMessage(workHead, receiver, deliveryTag, messageContent);
    }

    protected Delivery receiveMessageFromClient(String deliveryTag, String messageContent)
    {
        final Receiver receiver = getServer().receiver;
        final Delivery workHead = getServer().connection.getWorkHead();

        return receiveMessage(workHead, receiver, deliveryTag, messageContent);
    }

    protected Delivery receiveMessage(Delivery delivery, Receiver receiver, String deliveryTag, String messageContent)
    {
        assertTrue(Arrays.equals(deliveryTag.getBytes(StandardCharsets.UTF_8), delivery.getTag()));
        assertEquals("The received delivery should be on our receiver",
                            receiver, delivery.getLink());

        assertNull(delivery.getLocalState());
        assertNull(delivery.getRemoteState());

        assertFalse(delivery.isPartial());
        assertTrue(delivery.isReadable());

        final int BUFFER_SIZE = messageContent.length() * 4;
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
}
