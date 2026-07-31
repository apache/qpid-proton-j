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

import static org.apache.qpid.proton.systemtests.TestLoggingHelper.bold;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.security.SaslFrameBody;
import org.apache.qpid.proton.amqp.security.SaslInit;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.SymbolTypeTest;
import org.apache.qpid.proton.engine.Sasl;
import org.apache.qpid.proton.engine.SaslListener;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportDecodeException;
import org.apache.qpid.proton.engine.TransportException;
import org.apache.qpid.proton.engine.impl.AmqpFramer;
import org.apache.qpid.proton.engine.impl.AmqpHeader;
import org.apache.qpid.proton.engine.impl.ProtocolTracer;
import org.apache.qpid.proton.engine.impl.TransportImpl;
import org.apache.qpid.proton.framing.TransportFrame;
import org.apache.qpid.proton.engine.Sasl.SaslOutcome;

public class SaslTest extends EngineTestBase
{
    private static final Logger LOGGER = Logger.getLogger(SaslTest.class.getName());
    private static final String TESTMECH1 = "TESTMECH1";
    private static final String TESTMECH2 = "TESTMECH2";
    private static final byte[] INITIAL_RESPONSE_BYTES = "initial-response-bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] CHALLENGE_BYTES = "challenge-bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RESPONSE_BYTES = "response-bytes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ADDITIONAL_DATA_BYTES = "additional-data-bytes".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testSaslHostnamePropagationAndRetrieval() throws Exception
    {
        LOGGER.fine(bold("======== About to create transports"));

        getClient().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getClient().transport, TestLoggingHelper.CLIENT_PREFIX);

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();

        // Set the server hostname we are connecting to from the client
        String hostname = "my-remote-host-123";
        clientSasl.setRemoteHostname(hostname);

        // Verify we can't get the hostname on the client
        try
        {
            clientSasl.getHostname();
            fail("should have throw IllegalStateException");
        }
        catch (IllegalStateException ise)
        {
            // expected
        }

        getServer().transport = Proton.transport();
        ProtocolTracerEnabler.setProtocolTracer(getServer().transport, "            " + TestLoggingHelper.SERVER_PREFIX);

        // Configure the server to do ANONYMOUS
        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms("ANONYMOUS");

        // Verify we can't set the hostname on the server
        try
        {
            serverSasl.setRemoteHostname("some-other-host");
            fail("should have throw IllegalStateException");
        }
        catch (IllegalStateException ise)
        {
            // expected
        }

        assertNull(serverSasl.getHostname());
        assertArrayEquals(new String[0], clientSasl.getRemoteMechanisms());

        pumpClientToServer();
        pumpServerToClient();

        // Verify we got the mechs, set the chosen mech, and verify the
        // server still doesnt know the hostname set/requested by the client
        assertArrayEquals(new String[] {"ANONYMOUS"} , clientSasl.getRemoteMechanisms());
        clientSasl.setMechanisms("ANONYMOUS");
        assertNull(serverSasl.getHostname());

        pumpClientToServer();

        // Verify the server now knows that the client set the hostname field
        assertEquals(hostname, serverSasl.getHostname());
    }

    /** 5.3.2 SASL Negotiation. */
    @Test
    public void testSaslNegotiation() throws Exception
    {
        getClient().transport = Proton.transport();
        getServer().transport = Proton.transport();

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());

        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms(TESTMECH1, TESTMECH2);
        assertEquals("Server should not yet know the remote's chosen mechanism.",
                     0,
                     serverSasl.getRemoteMechanisms().length);

        pumpClientToServer();
        pumpServerToClient();

        assertArrayEquals("Client should now know the server's mechanisms.",
                          new String[]{TESTMECH1, TESTMECH2},
                          clientSasl.getRemoteMechanisms());
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        clientSasl.setMechanisms(TESTMECH1);

        pumpClientToServer();

        assertArrayEquals("Server should now know the client's chosen mechanism.",
                          new String[]{TESTMECH1},
                          serverSasl.getRemoteMechanisms());

        serverSasl.send(CHALLENGE_BYTES, 0, CHALLENGE_BYTES.length);

        pumpServerToClient();

        byte[] clientReceivedChallengeBytes = new byte[clientSasl.pending()];
        clientSasl.recv(clientReceivedChallengeBytes, 0, clientReceivedChallengeBytes.length);

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        assertArrayEquals("Client should now know the server's challenge",
                          CHALLENGE_BYTES,
                          clientReceivedChallengeBytes);

        clientSasl.send(RESPONSE_BYTES, 0, RESPONSE_BYTES.length);

        pumpClientToServer();

        byte[] serverReceivedResponseBytes = new byte[serverSasl.pending()];
        serverSasl.recv(serverReceivedResponseBytes, 0, serverReceivedResponseBytes.length);

        assertArrayEquals("Server should now know the client's response",
                          RESPONSE_BYTES,
                          serverReceivedResponseBytes);

        serverSasl.done(SaslOutcome.PN_SASL_OK);
        pumpServerToClient();

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_OK, clientSasl.getOutcome());
    }

    /** 5.3.2 SASL Negotiation. ...challenge/response step can occur zero or more times*/
    @Test
    public void testOptionalChallengeResponseStepOmitted() throws Exception
    {
        getClient().transport = Proton.transport();
        getServer().transport = Proton.transport();

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());

        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms(TESTMECH1);
        assertEquals("Server should not yet know the remote's chosen mechanism.",
                     0,
                     serverSasl.getRemoteMechanisms().length);

        pumpClientToServer();
        pumpServerToClient();

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        clientSasl.setMechanisms(TESTMECH1);

        pumpClientToServer();

        serverSasl.done(SaslOutcome.PN_SASL_OK);
        pumpServerToClient();

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_OK, clientSasl.getOutcome());
    }

    /**
     *  5.3.3.5 The additional-data field carries additional data on successful authentication outcome as specified
     *  by the SASL specification [RFC4422].
     */
    @Test
    public void testOutcomeAdditionalData() throws Exception
    {
        getClient().transport = Proton.transport();
        getServer().transport = Proton.transport();

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());

        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms(TESTMECH1);

        pumpClientToServer();
        pumpServerToClient();

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        clientSasl.setMechanisms(TESTMECH1);

        pumpClientToServer();

        serverSasl.send(CHALLENGE_BYTES, 0, CHALLENGE_BYTES.length);

        pumpServerToClient();

        byte[] clientReceivedChallengeBytes = new byte[clientSasl.pending()];
        clientSasl.recv(clientReceivedChallengeBytes, 0, clientReceivedChallengeBytes.length);

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        clientSasl.send(RESPONSE_BYTES, 0, RESPONSE_BYTES.length);

        pumpClientToServer();

        byte[] serverReceivedResponseBytes = new byte[serverSasl.pending()];
        serverSasl.recv(serverReceivedResponseBytes, 0, serverReceivedResponseBytes.length);

        serverSasl.send(ADDITIONAL_DATA_BYTES, 0, ADDITIONAL_DATA_BYTES.length);
        serverSasl.done(SaslOutcome.PN_SASL_OK);
        pumpServerToClient();

        byte[] clientReceivedAdditionalDataBytes = new byte[clientSasl.pending()];
        clientSasl.recv(clientReceivedAdditionalDataBytes, 0, clientReceivedAdditionalDataBytes.length);

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_OK, clientSasl.getOutcome());
        assertArrayEquals("Client should now know the serrver's additional-data",
                          ADDITIONAL_DATA_BYTES,
                          clientReceivedAdditionalDataBytes);
    }

    /**
     *  5.3.3.6 Connection authentication failed due to an unspecified problem with the supplied credentials.
     */
    @Test
    public void testAuthenticationFails() throws Exception
    {
        getClient().transport = Proton.transport();
        getServer().transport = Proton.transport();

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());

        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms(TESTMECH1);

        pumpClientToServer();
        pumpServerToClient();

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        clientSasl.setMechanisms(TESTMECH1);

        pumpClientToServer();

        serverSasl.done(SaslOutcome.PN_SASL_AUTH);
        pumpServerToClient();
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_AUTH, clientSasl.getOutcome());

    }

    /*
     * Test that transports configured to do so are able to perform SASL process where frames are
     * exchanged larger than the 512byte min-max-frame-size.
     */
    @Test
    public void testSaslNegotiationWithConfiguredLargerFrameSize() throws Exception
    {
        final byte[] largeInitialResponseBytesOrig = fillBytes("initialResponse", 1431);
        final byte[] largeChallengeBytesOrig = fillBytes("challenge", 1375);
        final byte[] largeResponseBytesOrig = fillBytes("response", 1282);
        final byte[] largeAdditionalBytesOrig = fillBytes("additionalData", 1529);

        getClient().transport = Proton.transport();
        getServer().transport = Proton.transport();

        // Configure transports to allow for larger initial frame sizes
        getClient().transport.setInitialRemoteMaxFrameSize(2048);
        getServer().transport.setInitialRemoteMaxFrameSize(2048);

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();

        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();

        // Negotiate the mech
        serverSasl.setMechanisms(TESTMECH1, TESTMECH2);

        pumpClientToServer();
        pumpServerToClient();

        assertArrayEquals("Client should now know the server's mechanisms.", new String[] { TESTMECH1, TESTMECH2 },
                clientSasl.getRemoteMechanisms());
        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());

        // Select a mech, send large initial response along with it in sasl-init, verify server receives it
        clientSasl.setMechanisms(TESTMECH1);
        byte[] initialResponseBytes = Arrays.copyOf(largeInitialResponseBytesOrig, largeInitialResponseBytesOrig.length);
        clientSasl.send(initialResponseBytes, 0, initialResponseBytes.length);

        pumpClientToServer();

        assertArrayEquals("Server should now know the client's chosen mechanism.", new String[] { TESTMECH1 },
                serverSasl.getRemoteMechanisms());

        byte[] serverReceivedInitialResponseBytes = new byte[serverSasl.pending()];
        serverSasl.recv(serverReceivedInitialResponseBytes, 0, serverReceivedInitialResponseBytes.length);

        assertArrayEquals("Server should now know the clients initial response", largeInitialResponseBytesOrig,
                serverReceivedInitialResponseBytes);

        // Send a large challenge in a sasl-challenge, verify client receives it
        byte[] challengeBytes = Arrays.copyOf(largeChallengeBytesOrig, largeChallengeBytesOrig.length);
        serverSasl.send(challengeBytes, 0, challengeBytes.length);

        pumpServerToClient();

        byte[] clientReceivedChallengeBytes = new byte[clientSasl.pending()];
        clientSasl.recv(clientReceivedChallengeBytes, 0, clientReceivedChallengeBytes.length);

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, clientSasl.getOutcome());
        assertArrayEquals("Client should now know the server's challenge", largeChallengeBytesOrig,
                clientReceivedChallengeBytes);

        // Send a large response in a sasl-response, verify server receives it
        byte[] responseBytes = Arrays.copyOf(largeResponseBytesOrig, largeResponseBytesOrig.length);
        clientSasl.send(responseBytes, 0, responseBytes.length);

        pumpClientToServer();

        byte[] serverReceivedResponseBytes = new byte[serverSasl.pending()];
        serverSasl.recv(serverReceivedResponseBytes, 0, serverReceivedResponseBytes.length);

        assertArrayEquals("Server should now know the client's response", largeResponseBytesOrig, serverReceivedResponseBytes);

        // Send an outcome with large additional data in a sasl-outcome, verify client receives it
        byte[] additionalBytes = Arrays.copyOf(largeAdditionalBytesOrig, largeAdditionalBytesOrig.length);
        serverSasl.send(additionalBytes, 0, additionalBytes.length);
        serverSasl.done(SaslOutcome.PN_SASL_OK);
        pumpServerToClient();

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_OK, clientSasl.getOutcome());

        byte[] clientReceivedAdditionalBytes = new byte[clientSasl.pending()];
        clientSasl.recv(clientReceivedAdditionalBytes, 0, clientReceivedAdditionalBytes.length);

        assertArrayEquals("Client should now know the server's outcome additional data", largeAdditionalBytesOrig,
                clientReceivedAdditionalBytes);
    }

    @Test
    public void testSaslNegotiationWithoutConfiguredLargerFrameSize() throws Exception
    {
        final String mechName = "ANONYMOUS";
        final byte[] mechNameBytes = mechName.getBytes(StandardCharsets.UTF_8);
        final byte[] largeInitialResponseBytesOrig = fillBytes("initialResponse", 513);

        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);

        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put(EncodingCodes.SMALLULONG); // small-ulong
        tmpBuf.put((byte) 0x41L); // sasl-init descriptor
        tmpBuf.put(EncodingCodes.LIST32); // list32 primitive type constructor
        tmpBuf.putInt(4 + 1 + 4 + mechNameBytes.length + 1 + 4 + largeInitialResponseBytesOrig.length); // size
        tmpBuf.putInt(2); // count
        tmpBuf.put((byte) 0xB3); // sym32 descriptor
        tmpBuf.putInt(mechNameBytes.length); // 4 byte symbol length
        tmpBuf.put(mechNameBytes);
        tmpBuf.put((byte) (byte) 0xB0); // vbin32 primitive type constructor
        tmpBuf.putInt(largeInitialResponseBytesOrig.length); // size
        tmpBuf.put(largeInitialResponseBytesOrig);

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateSaslFrameRawBody(0, frameBodyBytes);

        assertTrue(frameBytes.length > (512 + 8 + 4));

        final ByteBuffer clientToServerBuffer = ByteBuffer.wrap(frameBytes);

        assertTrue(clientToServerBuffer.remaining() > (512 + 8 + 4));

        getServer().transport = Proton.transport();

        final List<SaslFrameBody> saslBodies = new ArrayList<>();
        setProtocolTracer((TransportImpl) getServer().transport, saslBodies);

        // Configure the server to do ANONYMOUS.
        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms(mechName);

        // Send SASL header to begin connection
        ByteBuffer saslHeaderBuffer = ByteBuffer.wrap(AmqpHeader.SASL_HEADER);
        pumpBufferFromClientToServer(saslHeaderBuffer, false);

        final int capacity = getServer().transport.capacity();
        assertEquals(512, capacity);

        int totalSizeOfFrame = clientToServerBuffer.limit();
        assertTrue(totalSizeOfFrame > (512 + 8 + 4));
        // We can only feed the transport 512 at a time, but the first 512
        // contains the frame size which will cause it to barf.
        clientToServerBuffer.limit(512);

        // Send the bytes for [part of] the frame, expect failure
        try {
            pumpBufferFromClientToServer(clientToServerBuffer, false);
            fail("Expected exception");
        } catch (TransportException te) {
            assertNotNull(te.getMessage());
            assertTrue(te.getMessage().contains("specified frame size 552 larger than maximum SASL frame size 512"));
        }

        assertEquals(0, saslBodies.size());
    }

    private byte[] fillBytes(String seedString, int length)
    {
        byte[] seed = seedString.getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = seed[i % seed.length];
        }

        return bytes;
    }

    @Test
    public void testSaslNegotiationUsingListener() throws Exception
    {
        getClient().transport = Proton.transport();
        getServer().transport = Proton.transport();

        AtomicBoolean mechanismsReceived = new AtomicBoolean();
        AtomicBoolean challengeReceived = new AtomicBoolean();
        AtomicBoolean outcomeReceived = new AtomicBoolean();

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();
        clientSasl.setListener(new ClientSaslHandling(mechanismsReceived, challengeReceived, outcomeReceived));

        AtomicBoolean initReceived = new AtomicBoolean();
        AtomicBoolean responseReceived = new AtomicBoolean();

        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms(TESTMECH1, TESTMECH2);
        serverSasl.setListener(new ServerSaslHandling(initReceived, responseReceived));

        pumpClientToServer();
        pumpServerToClient();

        assertTrue("mechanisms were not received by client", mechanismsReceived.get());
        assertFalse("init was received by server", initReceived.get());

        pumpClientToServer();

        assertTrue("init was not received by server", initReceived.get());
        assertFalse("challenge was received by client", challengeReceived.get());

        pumpServerToClient();

        assertTrue("challenge was not received by client", challengeReceived.get());
        assertFalse("response was received by server", responseReceived.get());

        pumpClientToServer();

        assertTrue("response was received by server", responseReceived.get());
        assertFalse("outcome was received by client", outcomeReceived.get());

        pumpServerToClient();

        assertTrue("outcome was received by client", outcomeReceived.get());

        assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_OK, clientSasl.getOutcome());
    }

    @Test
    public void testSaslDecodingDoesNotCacheLongerSymbols() throws Exception
    {
        final String longMechName = "LONGER_THAN_CACHEABLE_SYMBOL_MECHANISM_1";
        final byte[] symbolBytes = longMechName.getBytes(StandardCharsets.US_ASCII);

        doSaslDecodingDoesNotCacheLongerSymbolsTestImpl(longMechName, symbolBytes, false);
    }

    @Test
    public void testSaslDecodingDoesNotCacheLongerSymbolsSym32() throws Exception
    {
        final String longMechName = "LONGER_THAN_CACHEABLE_SYMBOL_MECHANISM_2";
        final byte[] symbolBytes = longMechName.getBytes(StandardCharsets.US_ASCII);
        final boolean forceSym32 = true;

        doSaslDecodingDoesNotCacheLongerSymbolsTestImpl(longMechName, symbolBytes, forceSym32);
    }

    @Test
    public void testSaslDecodingDoesNotCacheLongerSymbols384() throws Exception
    {
        final byte[] payloadSymbolBytes = SymbolTypeTest.createPayloadSymbolBytes("testSaslDecodingDoesNotCacheLongerSymbols384-", 384);
        final String longMechName = new String(payloadSymbolBytes, StandardCharsets.US_ASCII);

        doSaslDecodingDoesNotCacheLongerSymbolsTestImpl(longMechName, payloadSymbolBytes, false);
    }

    private void doSaslDecodingDoesNotCacheLongerSymbolsTestImpl(final String longMechName, final byte[] symbolBytes, final boolean forceSym32)
    {
        final Symbol chosenMechanism = doSaslDecodingCachingTestImpl(longMechName, symbolBytes, forceSym32);

        assertEquals(longMechName, chosenMechanism.toString());

        final Symbol lookupSasl = Symbol.getSaslSymbol(longMechName);
        final Symbol lookupMain = Symbol.getSymbol(longMechName);

        final Symbol chosenMechanism2 = doSaslDecodingCachingTestImpl(longMechName, symbolBytes, forceSym32);

        assertEquals(chosenMechanism, lookupSasl);
        assertNotSame(chosenMechanism, lookupSasl); // Exceeds Sasl cache limit

        assertEquals(chosenMechanism, lookupMain);
        assertNotSame(chosenMechanism, lookupMain);

        assertEquals(chosenMechanism, chosenMechanism2);
        assertNotSame(chosenMechanism, chosenMechanism2); // Exceeds Sasl cache limit
        assertNotSame(chosenMechanism2, lookupSasl); // Exceeds Sasl cache limit
    }

    @Test
    public void testSaslDecodingDoesCacheSmallerSymbols() throws Exception
    {
        final String smallerMechName = "CACHEABLE_MECH_A";
        final byte[] symbolBytes = smallerMechName.getBytes(StandardCharsets.US_ASCII);

        doSaslDecodingDoesCacheSmallerSymbolsTestImpl(smallerMechName, symbolBytes, false);
    }

    @Test
    public void testSaslDecodingDoesCacheSmallerSymbolsSym32() throws Exception
    {
        final String smallerMechName = "CACHEABLE_MECH_B";
        final byte[] symbolBytes = smallerMechName.getBytes(StandardCharsets.US_ASCII);
        final boolean forceSym32 = true;

        doSaslDecodingDoesCacheSmallerSymbolsTestImpl(smallerMechName, symbolBytes, forceSym32);
    }

    @Test
    @Ignore("Cache is static so running has side effects.")
    public void testSaslDecodingCachingMaxesOut() throws Exception
    {
        doSaslDecodingCachingMaxesOutTestImpl("CACHEABLE_MECH_", false);
    }

    @Test
    @Ignore("Cache is static so running has side effects.")
    public void testSaslDecodingCachingMaxesOutSym32() throws Exception
    {
        doSaslDecodingCachingMaxesOutTestImpl("CACHEABLE_MECH_32_", true);
    }

    private void doSaslDecodingCachingMaxesOutTestImpl(final String smallerMechNamePrefix, final boolean forceSym32)
    {
        boolean notSame = false;
        int attempts = 0;

        for (attempts = 0; attempts <= Short.MAX_VALUE; attempts++)
        {
            final String mechanismName = smallerMechNamePrefix + String.valueOf(attempts);
            final byte[] symbolBytes = mechanismName.getBytes(StandardCharsets.US_ASCII);

            final Symbol symbol1 = doSaslDecodingCachingTestImpl(mechanismName, symbolBytes, forceSym32);
            final Symbol symbol2 = doSaslDecodingCachingTestImpl(mechanismName, symbolBytes, forceSym32);

            if (symbol1 != symbol2)
            {
                notSame = true;
                break;
            }
        }

        assertTrue(notSame);
        assertTrue(attempts > 5);
        assertTrue(attempts <= 64);
    }

    private void doSaslDecodingDoesCacheSmallerSymbolsTestImpl(final String smallerMechName, final byte[] symbolBytes, final boolean forceSym32)
    {
        final Symbol chosenMechanism = doSaslDecodingCachingTestImpl(smallerMechName, symbolBytes, forceSym32);

        assertEquals(smallerMechName, chosenMechanism.toString());

        final Symbol lookupSasl = Symbol.getSaslSymbol(smallerMechName);
        final Symbol lookupMain = Symbol.getSymbol(smallerMechName);

        final Symbol chosenMechanism2 = doSaslDecodingCachingTestImpl(smallerMechName, symbolBytes, forceSym32);

        assertEquals(chosenMechanism, lookupSasl);
        assertSame(chosenMechanism, lookupSasl); // Below Sasl cache limit

        assertEquals(chosenMechanism, lookupMain);
        assertNotSame(chosenMechanism, lookupMain); // Sasl cant modify main cache

        assertEquals(chosenMechanism, chosenMechanism2);
        assertSame(chosenMechanism, chosenMechanism2); // Below Sasl cache limit
        assertSame(chosenMechanism2, lookupSasl); // Below Sasl cache limit
    }

    private Symbol doSaslDecodingCachingTestImpl(final String mechName, final byte[] mechSymbolBytes, final boolean forceSym32)
    {
        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put(EncodingCodes.SMALLULONG);
        tmpBuf.put((byte) 0x41); // sasl-init ulong descriptor
        if (mechName.length() >= (255 - 1 - 1) || forceSym32)
        {
            tmpBuf.put((byte) 0xD0); // list32
            final int size = (4 + 1 + 4) + mechName.length(); // (count + sym32 + length) + value-bytes
            tmpBuf.putInt(size); // 4 byte size
            tmpBuf.putInt(0x01); // 4 byte count
            tmpBuf.put((byte) 0xB3); // sym32 descriptor
            tmpBuf.putInt(mechName.length()); // 4 byte symbol length
        }
        else
        {
            tmpBuf.put((byte) 0xC0); // list8
            final int size = (1 + 1 + 1) + mechName.length(); // (count + sym8 + length) + value-bytes
            tmpBuf.put((byte) size); // 1 byte size
            tmpBuf.put((byte) 0x01); // 1 byte count
            tmpBuf.put((byte) 0xA3); // sym8 descriptor
            tmpBuf.put((byte) mechName.length()); // 1 byte symbol length
        }
        tmpBuf.put(mechSymbolBytes);
        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateSaslFrameRawBody(0, frameBodyBytes);

        final ByteBuffer clientToServerBuffer = ByteBuffer.wrap(frameBytes);

        getServer().transport = Proton.transport();

        final List<SaslFrameBody> saslBodies = new ArrayList<>();
        setProtocolTracer((TransportImpl) getServer().transport, saslBodies);

        // Configure the server to do ANONYMOUS.
        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms("ANONYMOUS");

        // Send SASL header to begin connection
        ByteBuffer saslHeaderBuffer = ByteBuffer.wrap(AmqpHeader.SASL_HEADER);
        pumpBufferFromClientToServer(saslHeaderBuffer, false);

        // Send the bytes for the sasl-init selecting the mechanism
        pumpBufferFromClientToServer(clientToServerBuffer, false);

        // Verify expected frame details decoded
        assertEquals(1, saslBodies.size());
        SaslFrameBody saslFrameBody = saslBodies.get(0);
        assertNotNull(saslFrameBody);
        assertEquals(SaslInit.class, saslFrameBody.getClass());
        SaslInit saslInit = (SaslInit) saslFrameBody;

        assertNotNull(saslInit.getMechanism());
        final Symbol chosenMechanism = saslInit.getMechanism();
        assertEquals(mechName, chosenMechanism.toString());
        return chosenMechanism;
    }

    @Test
    public void testSaslDecodingRejectsArrayOfZeroWidthElement() throws Exception
    {
        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put(EncodingCodes.SMALLULONG);
        tmpBuf.put((byte) 0x40); // sasl-mechanisms ulong descriptor
        tmpBuf.put((byte) 0xC0); // list8
        tmpBuf.put((byte) (1 + 1 + 1 + 1 + 1));
        tmpBuf.put((byte) 1);
        tmpBuf.put((byte) 0xE0); // 'array8' type descriptor code
        tmpBuf.put((byte) (1 + 1));
        tmpBuf.put((byte) 1);
        tmpBuf.put(EncodingCodes.BOOLEAN_TRUE);

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateSaslFrameRawBody(0, frameBodyBytes);

        final ByteBuffer serverToClientBuffer = ByteBuffer.wrap(frameBytes);

        getClient().transport = Proton.transport();

        final List<SaslFrameBody> saslBodies = new ArrayList<>();
        setProtocolTracer((TransportImpl) getClient().transport, saslBodies);

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();

        // Send SASL header to begin connection
        ByteBuffer saslHeaderBuffer = ByteBuffer.wrap(AmqpHeader.SASL_HEADER);
        pumpBufferFromServerToClient(saslHeaderBuffer, false);

        // Send the bytes for the frame, expect failure
        try {
            pumpBufferFromServerToClient(serverToClientBuffer, false);
           fail("Expected exception");
        } catch (TransportException te) {
            assertNotNull(te.getCause());
            assertTrue(te.getCause() instanceof TransportDecodeException);
            final TransportDecodeException tde = (TransportDecodeException) (te.getCause());

            assertNotNull(tde.getMessage());
            assertEquals("Array element count 1 is specified to be greater than the allowed 0-width-element limit for arrays (0)", tde.getMessage());
        }

        assertEquals(0, saslBodies.size());
    }

    @Test
    public void testSaslDecodingRejectsUnexpectedDepth() throws Exception
    {
        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put(EncodingCodes.SMALLULONG);
        tmpBuf.put((byte) 0x40); // sasl-mechanisms ulong descriptor
        tmpBuf.put((byte) 0xC0); // list8
        tmpBuf.put((byte) (1 + 1 + 1 + 1 + 1 + 4 + 4));
        tmpBuf.put((byte) 1);
        tmpBuf.put((byte) 0xE0); // 'array8' type descriptor code
        tmpBuf.put((byte) (1 + 1 + 4 + 4));
        tmpBuf.put((byte) 1);
        tmpBuf.put((byte) 0xD0);
        tmpBuf.putInt(4);
        tmpBuf.putInt(0);

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateSaslFrameRawBody(0, frameBodyBytes);

        final ByteBuffer serverToClientBuffer = ByteBuffer.wrap(frameBytes);

        getClient().transport = Proton.transport();

        final List<SaslFrameBody> saslBodies = new ArrayList<>();
        setProtocolTracer((TransportImpl) getClient().transport, saslBodies);

        Sasl clientSasl = getClient().transport.sasl();
        clientSasl.client();

        // Send SASL header to begin connection
        ByteBuffer saslHeaderBuffer = ByteBuffer.wrap(AmqpHeader.SASL_HEADER);
        pumpBufferFromServerToClient(saslHeaderBuffer, false);

        // Send the bytes for the frame, expect failure
        try {
            pumpBufferFromServerToClient(serverToClientBuffer, false);
           fail("Expected exception");
        } catch (TransportException te) {
            assertNotNull(te.getCause());
            assertTrue(te.getCause() instanceof TransportDecodeException);
            final TransportDecodeException tde = (TransportDecodeException) (te.getCause());

            assertNotNull(tde.getMessage());
            assertEquals("The nesting of types in the object being decoded exceeded the configured limit: 2", tde.getMessage());
        }

        assertEquals(0, saslBodies.size());
    }

    @Test
    public void testSaslDescribedTypeHandlingRejectsReservedDescriptor() throws Exception
    {
        final String descriptorValue = "DESCRIPTOR";
        final byte[] bytes = descriptorValue.getBytes(StandardCharsets.US_ASCII);
        assertTrue(bytes.length <= 255);

        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put(EncodingCodes.STR8); // String, reserved
        tmpBuf.put((byte) bytes.length); // descriptor length
        tmpBuf.put(bytes); // descriptor value
        tmpBuf.put((byte) 0x45); // list0 primitive type constructor

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateSaslFrameRawBody(0, frameBodyBytes);

        final ByteBuffer clientToServerBuffer = ByteBuffer.wrap(frameBytes);

        getServer().transport = Proton.transport();

        final List<SaslFrameBody> saslBodies = new ArrayList<>();
        setProtocolTracer((TransportImpl) getServer().transport, saslBodies);

        // Configure the server to do ANONYMOUS.
        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms("ANONYMOUS");

        // Send SASL header to begin connection
        ByteBuffer saslHeaderBuffer = ByteBuffer.wrap(AmqpHeader.SASL_HEADER);
        pumpBufferFromClientToServer(saslHeaderBuffer, false);

        // Send the bytes for the frame with reserved descriptor in body, expect failure
        try {
           pumpBufferFromClientToServer(clientToServerBuffer, false);
           fail("Expected exception");
        } catch (TransportException te) {
            assertNotNull(te.getCause());
            assertTrue(te.getCause() instanceof TransportDecodeException);
            final TransportDecodeException tde = (TransportDecodeException) (te.getCause());

            assertNotNull(tde.getMessage());
            assertEquals("Unexpected described type descriptor encoding during SASL: STR8:0xa1", tde.getMessage());
        }

        assertEquals(0, saslBodies.size());
    }

    @Test
    public void testSaslDescribedTypeHandlingRejectsUnknownDescriptor() throws Exception
    {
        final String descriptorValue = "UNKNOWN";
        final byte[] bytes = descriptorValue.getBytes(StandardCharsets.US_ASCII);
        assertTrue(bytes.length <= 255);

        AmqpFramer amqpFramer = new AmqpFramer();

        ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
        tmpBuf.put((byte) 0x00); // DescribedType
        tmpBuf.put(EncodingCodes.SYM8); // Symbol
        tmpBuf.put((byte) bytes.length); // descriptor length
        tmpBuf.put(bytes); // descriptor value
        tmpBuf.put((byte) 0x45); // list0 primitive type constructor

        tmpBuf.flip();

        final byte[] frameBodyBytes = ByteBuffer.allocate(tmpBuf.remaining()).put(tmpBuf).array();
        final byte[] frameBytes = amqpFramer.generateSaslFrameRawBody(0, frameBodyBytes);

        final ByteBuffer clientToServerBuffer = ByteBuffer.wrap(frameBytes);

        getServer().transport = Proton.transport();

        final List<SaslFrameBody> saslBodies = new ArrayList<>();
        setProtocolTracer((TransportImpl) getServer().transport, saslBodies);

        // Configure the server to do ANONYMOUS.
        Sasl serverSasl = getServer().transport.sasl();
        serverSasl.server();
        serverSasl.setMechanisms("ANONYMOUS");

        // Send SASL header to begin connection
        ByteBuffer saslHeaderBuffer = ByteBuffer.wrap(AmqpHeader.SASL_HEADER);
        pumpBufferFromClientToServer(saslHeaderBuffer, false);

        // Send the bytes for the frame with reserved descriptor in body, expect failure
        try {
           pumpBufferFromClientToServer(clientToServerBuffer, false);
           fail("Expected exception");
        } catch (TransportException te) {
            assertNotNull(te.getCause());
            assertTrue(te.getCause() instanceof TransportDecodeException);
            final TransportDecodeException tde = (TransportDecodeException) (te.getCause());

            assertNotNull(tde.getMessage());
            assertEquals("Cannot decode unknown described types during SASL", tde.getMessage());
        }

        assertEquals(0, saslBodies.size());
    }

    private void setProtocolTracer(final TransportImpl transport, final List<SaslFrameBody> saslBodies)
    {
        transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void receivedSaslBody(final SaslFrameBody saslFrameBody)
            {
                saslBodies.add(saslFrameBody);
            }

            @Override
            public void receivedFrame(TransportFrame transportFrame) { }

            @Override
            public void sentFrame(TransportFrame transportFrame) { }
        });
    }

    private static class ServerSaslHandling implements SaslListener
    {
        AtomicBoolean initReceived = new AtomicBoolean();
        AtomicBoolean responseReceived = new AtomicBoolean();

        public ServerSaslHandling(AtomicBoolean initReceived, AtomicBoolean responseReceived)
        {
            this.initReceived = initReceived;
            this.responseReceived = responseReceived;
        }

        @Override
        public void onSaslInit(Sasl s, Transport t)
        {
            assertArrayEquals("Server should now know the client's chosen mechanism.",
                    new String[]{TESTMECH1}, s.getRemoteMechanisms());

            byte[] serverReceivedInitialBytes = new byte[s.pending()];
            s.recv(serverReceivedInitialBytes, 0, serverReceivedInitialBytes.length);

            assertArrayEquals("Server should now know the client's initial response.",
                    INITIAL_RESPONSE_BYTES, serverReceivedInitialBytes);

            s.send(CHALLENGE_BYTES, 0, CHALLENGE_BYTES.length);

            assertFalse("Should not have already received init", initReceived.getAndSet(true));
        }

        @Override
        public void onSaslResponse(Sasl s, Transport t)
        {
            byte[] serverReceivedResponseBytes = new byte[s.pending()];
            s.recv(serverReceivedResponseBytes, 0, serverReceivedResponseBytes.length);

            assertArrayEquals("Server should now know the client's response", RESPONSE_BYTES, serverReceivedResponseBytes);

            s.send(ADDITIONAL_DATA_BYTES, 0, ADDITIONAL_DATA_BYTES.length);
            s.done(SaslOutcome.PN_SASL_OK);

            assertFalse("Should not have already received response", responseReceived.getAndSet(true));
        }

        @Override
        public void onSaslMechanisms(Sasl s, Transport t) { }

        @Override
        public void onSaslChallenge(Sasl s, Transport t) { }

        @Override
        public void onSaslOutcome(Sasl s, Transport t) { }
    }

    private static class ClientSaslHandling implements SaslListener
    {
        AtomicBoolean mechanismsReceived = new AtomicBoolean();
        AtomicBoolean challengeReceived = new AtomicBoolean();
        AtomicBoolean outcomeReceived = new AtomicBoolean();

        public ClientSaslHandling(AtomicBoolean mechanismsReceived, AtomicBoolean challengeReceived, AtomicBoolean outcomeReceived)
        {
            this.mechanismsReceived = mechanismsReceived;
            this.challengeReceived = challengeReceived;
            this.outcomeReceived = outcomeReceived;
        }

        @Override
        public void onSaslMechanisms(Sasl s, Transport t)
        {
            assertArrayEquals("Client should now know the server's mechanisms.",
                    new String[]{TESTMECH1, TESTMECH2}, s.getRemoteMechanisms());
            assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, s.getOutcome());

            s.setMechanisms(TESTMECH1);
            s.send(INITIAL_RESPONSE_BYTES, 0, INITIAL_RESPONSE_BYTES.length);

            assertFalse("Should not have already received mechanisms", mechanismsReceived.getAndSet(true));
        }

        @Override
        public void onSaslChallenge(Sasl s, Transport t)
        {
            byte[] clientReceivedChallengeBytes = new byte[s.pending()];
            s.recv(clientReceivedChallengeBytes, 0, clientReceivedChallengeBytes.length);

            assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_NONE, s.getOutcome());
            assertArrayEquals("Client should now know the server's challenge",
                              CHALLENGE_BYTES, clientReceivedChallengeBytes);

            s.send(RESPONSE_BYTES, 0, RESPONSE_BYTES.length);

            assertFalse("Should not have already received challenge", challengeReceived.getAndSet(true));
        }

        @Override
        public void onSaslOutcome(Sasl s, Transport t)
        {
            assertEquals("Unexpected SASL outcome at client", SaslOutcome.PN_SASL_OK, s.getOutcome());

            byte[] clientReceivedAdditionalBytes = new byte[s.pending()];
            s.recv(clientReceivedAdditionalBytes, 0, clientReceivedAdditionalBytes.length);

            assertArrayEquals("Client should now know the server's outcome additional data", clientReceivedAdditionalBytes,
                    clientReceivedAdditionalBytes);

            assertFalse("Should not have already received outcome", outcomeReceived.getAndSet(true));
        }

        @Override
        public void onSaslInit(Sasl s, Transport t) { }

        @Override
        public void onSaslResponse(Sasl s, Transport t) { }
    }
}
