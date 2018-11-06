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
package org.apache.qpid.proton.systemtests.engine;

import static org.apache.qpid.proton.engine.EndpointState.ACTIVE;
import static org.apache.qpid.proton.engine.EndpointState.UNINITIALIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Endpoint;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.SslDomain;
import org.apache.qpid.proton.engine.SslDomain.Mode;
import org.apache.qpid.proton.engine.SslDomain.VerifyMode;
import org.apache.qpid.proton.engine.SslPeerDetails;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportException;
import org.junit.Test;

public class SslTest
{
    private static final String SERVER_JKS_KEYSTORE = "src/test/resources/server-jks.keystore";
    private static final String SERVER_JKS_TRUSTSTORE = "src/test/resources/server-jks.truststore";
    private static final String CLIENT_JKS_KEYSTORE = "src/test/resources/client-jks.keystore";
    private static final String CLIENT_JKS_TRUSTSTORE = "src/test/resources/client-jks.truststore";
    private static final String PASSWORD = "password";

    private static final String SERVER_2_JKS_KEYSTORE = "src/test/resources/server2-jks.keystore";
    private static final String CA_CERTS = "src/test/resources/ca-certs.crt";

    private static final String SERVER_CONTAINER = "serverContainer";
    private static final String CLIENT_CONTAINER = "clientContainer";

    private final Transport _clientTransport = Proton.transport();
    private final Transport _serverTransport = Proton.transport();

    private final TransportPumper _pumper = new TransportPumper(_clientTransport, _serverTransport);

    private final Connection _clientConnection = Proton.connection();
    private final Connection _serverConnection = Proton.connection();

    @Test
    public void testFailureToInitSslDomainThrowsISE() throws Exception {
        SslDomain sslDomain = SslDomain.Factory.create();

        try {
            _clientTransport.ssl(sslDomain, null);
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException ise) {
            // Expected
            assertTrue(ise.getMessage().contains("Client/server mode must be configured"));
        }

        try {
            _serverTransport.ssl(sslDomain);
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException ise) {
            // Expected
            assertTrue(ise.getMessage().contains("Client/server mode must be configured"));
        }
    }

    @Test
    public void testOpenConnectionsWithProvidedSslContext() throws Exception
    {
        SslDomain clientSslDomain = SslDomain.Factory.create();
        clientSslDomain.init(Mode.CLIENT);
        clientSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER);
        SSLContext clientSslContext = createSslContext(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        clientSslDomain.setSslContext(clientSslContext);
        _clientTransport.ssl(clientSslDomain);

        SslDomain serverSslDomain = SslDomain.Factory.create();
        serverSslDomain.init(Mode.SERVER);
        serverSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER);
        SSLContext serverSslContext = createSslContext(SERVER_JKS_KEYSTORE, PASSWORD, SERVER_JKS_TRUSTSTORE, PASSWORD);
        serverSslDomain.setSslContext(serverSslContext);
        _serverTransport.ssl(serverSslDomain);

        _clientConnection.setContainer(CLIENT_CONTAINER);
        _serverConnection.setContainer(SERVER_CONTAINER);

        _clientTransport.bind(_clientConnection);
        _serverTransport.bind(_serverConnection);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);

        _clientConnection.open();

        assertEndpointState(_clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(_serverConnection, UNINITIALIZED, UNINITIALIZED);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);

        _pumper.pumpAll();

        assertEndpointState(_clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(_serverConnection, UNINITIALIZED, ACTIVE);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);

        _serverConnection.open();

        assertEndpointState(_clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(_serverConnection, ACTIVE, ACTIVE);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);

        _pumper.pumpAll();

        assertEndpointState(_clientConnection, ACTIVE, ACTIVE);
        assertEndpointState(_serverConnection, ACTIVE, ACTIVE);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);
    }

    @Test
    public void testHostnameVerificationSuccess() throws Exception {
        doHostnameVerificationTestImpl(true, true);
    }

    @Test
    public void testHostnameVerificationFailure() throws Exception {
        doHostnameVerificationTestImpl(false, true);
    }

    @Test
    public void testHostnameVerificationSkipped() throws Exception {
        doHostnameVerificationTestImpl(false, false);
    }

    private void doHostnameVerificationTestImpl(boolean useMatchingServerName, boolean useHostnameVerification) throws Exception {
        final String serverPeerName = useMatchingServerName ? "localhost" : "anotherserverhost";
        final VerifyMode clientVerifyMode = useHostnameVerification ? VerifyMode.VERIFY_PEER_NAME : VerifyMode.VERIFY_PEER;

        SslDomain clientSslDomain = SslDomain.Factory.create();
        clientSslDomain.init(Mode.CLIENT);
        clientSslDomain.setPeerAuthentication(clientVerifyMode);

        SSLContext clientSslContext = createSslContext(CLIENT_JKS_KEYSTORE, PASSWORD, CLIENT_JKS_TRUSTSTORE, PASSWORD);
        clientSslDomain.setSslContext(clientSslContext);
        SslPeerDetails sslPeerDetails = SslPeerDetails.Factory.create(serverPeerName, 1234);
        _clientTransport.ssl(clientSslDomain, sslPeerDetails);

        SslDomain serverSslDomain = SslDomain.Factory.create();
        serverSslDomain.init(Mode.SERVER);
        serverSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER_NAME);
        SSLContext serverSslContext = createSslContext(SERVER_JKS_KEYSTORE, PASSWORD, SERVER_JKS_TRUSTSTORE, PASSWORD);
        serverSslDomain.setSslContext(serverSslContext);
        _serverTransport.ssl(serverSslDomain, SslPeerDetails.Factory.create("client", 4567));

        _clientConnection.setContainer(CLIENT_CONTAINER);
        _serverConnection.setContainer(SERVER_CONTAINER);

        _clientTransport.bind(_clientConnection);
        _serverTransport.bind(_serverConnection);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);

        _clientConnection.open();

        assertEndpointState(_clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(_serverConnection, UNINITIALIZED, UNINITIALIZED);

        assertConditions(_clientTransport);
        assertConditions(_serverTransport);

        if(useHostnameVerification && !useMatchingServerName) {
            // Verify the expected failures and resulting transport closures
            pumpWithFailingNegotiation();

            assertEquals(Transport.END_OF_STREAM, _clientTransport.pending());
            assertEquals(Transport.END_OF_STREAM, _clientTransport.capacity());

            assertEquals(Transport.END_OF_STREAM, _serverTransport.pending());
            assertEquals(Transport.END_OF_STREAM, _serverTransport.capacity());
            return;
        } else {
            // Verify the connections succeed
            _pumper.pumpAll();

            assertEndpointState(_clientConnection, ACTIVE, UNINITIALIZED);
            assertEndpointState(_serverConnection, UNINITIALIZED, ACTIVE);

            assertConditions(_clientTransport);
            assertConditions(_serverTransport);

            _serverConnection.open();

            assertEndpointState(_clientConnection, ACTIVE, UNINITIALIZED);
            assertEndpointState(_serverConnection, ACTIVE, ACTIVE);

            assertConditions(_clientTransport);
            assertConditions(_serverTransport);

            _pumper.pumpAll();

            assertEndpointState(_clientConnection, ACTIVE, ACTIVE);
            assertEndpointState(_serverConnection, ACTIVE, ACTIVE);

            assertConditions(_clientTransport);
            assertConditions(_serverTransport);
        }
    }

    @Test
    public void testOmittingPeerDetailsThrowsIAEWhenRequired() throws Exception {
        doOmitPeerDetailsThrowsIAEWhenRequiredTestImpl(true);
        doOmitPeerDetailsThrowsIAEWhenRequiredTestImpl(false);
    }

    private void doOmitPeerDetailsThrowsIAEWhenRequiredTestImpl(boolean explicitlySetVerifyMode) {
        SslDomain clientSslDomain = SslDomain.Factory.create();
        clientSslDomain.init(Mode.CLIENT);

        if (explicitlySetVerifyMode) {
            clientSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER_NAME);
        }

        try {
            _clientTransport.ssl(clientSslDomain, null);
            fail("Expected an exception to be thrown");
        } catch (IllegalArgumentException ise) {
            // Expected
        }

        try {
            _clientTransport.ssl(clientSslDomain);
            fail("Expected an exception to be thrown");
        } catch (IllegalArgumentException ise) {
            // Expected
        }
    }

    private void pumpWithFailingNegotiation() throws Exception {
        try {
            _pumper.pumpAll();
            fail("Expected an exception");
        } catch (TransportException te) {
            assertTrue(te.getCause().getCause() instanceof SSLHandshakeException);
        }

        try {
            _pumper.pumpAll();
            fail("Expected an exception");
        } catch (TransportException te) {
            assertTrue(te.getCause().getCause() instanceof SSLException);
        }

        _pumper.pumpAll();
    }

    private void assertConditions(Transport transport)
    {
        ErrorCondition remoteCondition = transport.getRemoteCondition();
        if (remoteCondition != null)
        {
            assertNull(remoteCondition.getCondition());
        }

        ErrorCondition condition = transport.getCondition();
        if (condition != null)
        {
            assertNull(condition.getCondition());
        }
    }

    private SSLContext createSslContext(String keyStoreLocation, String keyStorePassword,
                                        String trustStoreLocation, String trustStorePassword) throws Exception
    {
        SSLContext context = SSLContext.getInstance("TLS");

        KeyManagerFactory keyFact = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        TrustManagerFactory trustFact = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream in = new FileInputStream(new File(keyStoreLocation));)
        {
            keyStore.load(in, keyStorePassword.toCharArray());
        }
        keyFact.init(keyStore, keyStorePassword.toCharArray());

        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (InputStream in = new FileInputStream(new File(trustStoreLocation));)
        {
            trustStore.load(in, trustStorePassword.toCharArray());
        }
        trustFact.init(trustStore);

        context.init(keyFact.getKeyManagers(), trustFact.getTrustManagers(), new SecureRandom());

        return context;
    }

    private void assertEndpointState(Endpoint endpoint, EndpointState localState, EndpointState remoteState)
    {
        assertEquals("Unexpected local state", localState, endpoint.getLocalState());
        assertEquals("Unexpected remote state", remoteState, endpoint.getRemoteState());
    }

    @Test
    public void testMultiplePemTrustCertificates() throws Exception
    {
        doMultiplePemTrustCertificatesTestImpl(SERVER_JKS_KEYSTORE);
        doMultiplePemTrustCertificatesTestImpl(SERVER_2_JKS_KEYSTORE);
    }

    private void doMultiplePemTrustCertificatesTestImpl(String serverKeystore) throws Exception {
        Transport clientTransport = Proton.transport();
        Transport serverTransport = Proton.transport();

        TransportPumper pumper = new TransportPumper(clientTransport, serverTransport);

        Connection clientConnection = Proton.connection();
        Connection serverConnection = Proton.connection();

        SslDomain clientSslDomain = SslDomain.Factory.create();
        clientSslDomain.init(Mode.CLIENT);
        clientSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER);
        clientSslDomain.setTrustedCaDb(CA_CERTS);
        clientTransport.ssl(clientSslDomain);

        SslDomain serverSslDomain = SslDomain.Factory.create();
        serverSslDomain.init(Mode.SERVER);
        SSLContext serverSslContext = createSslContext(serverKeystore, PASSWORD, SERVER_JKS_TRUSTSTORE, PASSWORD);
        serverSslDomain.setSslContext(serverSslContext);
        serverTransport.ssl(serverSslDomain);

        clientConnection.setContainer(CLIENT_CONTAINER);
        serverConnection.setContainer(SERVER_CONTAINER);

        clientTransport.bind(clientConnection);
        serverTransport.bind(serverConnection);

        assertConditions(clientTransport);
        assertConditions(serverTransport);

        clientConnection.open();

        assertEndpointState(clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(serverConnection, UNINITIALIZED, UNINITIALIZED);

        assertConditions(clientTransport);
        assertConditions(serverTransport);

        pumper.pumpAll();

        assertEndpointState(clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(serverConnection, UNINITIALIZED, ACTIVE);

        assertConditions(clientTransport);
        assertConditions(serverTransport);

        serverConnection.open();

        assertEndpointState(clientConnection, ACTIVE, UNINITIALIZED);
        assertEndpointState(serverConnection, ACTIVE, ACTIVE);

        assertConditions(clientTransport);
        assertConditions(serverTransport);

        pumper.pumpAll();

        assertEndpointState(clientConnection, ACTIVE, ACTIVE);
        assertEndpointState(serverConnection, ACTIVE, ACTIVE);

        assertConditions(clientTransport);
        assertConditions(serverTransport);
    }
}
