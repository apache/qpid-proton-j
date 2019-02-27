/*
 *
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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.security.SaslFrameBody;
import org.apache.qpid.proton.amqp.security.SaslMechanisms;
import org.apache.qpid.proton.framing.TransportFrame;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class SaslImplTest {

    @Test
    public void testPlainHelperEncodesExpectedResponse() {
        TransportImpl transport = new TransportImpl();
        SaslImpl sasl = new SaslImpl(transport, 512);

        // Use a username + password with a unicode char that encodes
        // differently under changing charsets
        String username = "username-with-unicode" + "\u1F570";
        String password = "password-with-unicode" + "\u1F570";

        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);

        byte[] expectedResponseBytes = new byte[usernameBytes.length + passwordBytes.length + 2];
        System.arraycopy(usernameBytes, 0, expectedResponseBytes, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, expectedResponseBytes, 2 + usernameBytes.length, passwordBytes.length);

        sasl.plain(username, password);

        assertEquals("Unexpected response data", new Binary(expectedResponseBytes), sasl.getChallengeResponse());
    }

    @Test
    public void testProtocolTracingLogsToTracer() {
        TransportImpl transport = new TransportImpl();
        List<SaslFrameBody> bodies = new ArrayList<>();
        transport.setProtocolTracer(new ProtocolTracer()
        {
            @Override
            public void receivedSaslBody(final SaslFrameBody saslFrameBody)
            {
                bodies.add(saslFrameBody);
            }

            @Override
            public void receivedFrame(TransportFrame transportFrame) { }

            @Override
            public void sentFrame(TransportFrame transportFrame) { }
        });

        SaslImpl sasl = new SaslImpl(transport, 512);

        SaslMechanisms mechs = new SaslMechanisms();
        mechs.setSaslServerMechanisms(Symbol.valueOf("TESTMECH"));

        assertEquals(0, bodies.size());
        sasl.handle(mechs, null);
        assertEquals(1, bodies.size());
        assertTrue(bodies.get(0) instanceof SaslMechanisms);
    }

    @Test
    public void testProtocolTracingLogsToSystem() {
        TransportImpl transport = new TransportImpl();
        transport.trace(2);

        TransportImpl spy = spy(transport);

        final String testMechName = "TESTMECH";
        SaslMechanisms mechs = new SaslMechanisms();
        mechs.setSaslServerMechanisms(Symbol.valueOf(testMechName));

        SaslImpl sasl = new SaslImpl(spy, 512);

        sasl.handle(mechs, null);

        ArgumentCaptor<SaslMechanisms> frameBodyCatcher = ArgumentCaptor.forClass(SaslMechanisms.class);
        verify(spy).log(eq(TransportImpl.INCOMING), frameBodyCatcher.capture());

        Symbol[] expectedMechs = new Symbol[] { Symbol.valueOf(testMechName)};
        assertArrayEquals(expectedMechs, frameBodyCatcher.getValue().getSaslServerMechanisms());
    }
}
