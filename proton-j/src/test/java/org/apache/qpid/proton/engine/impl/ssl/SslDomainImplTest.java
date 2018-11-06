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
package org.apache.qpid.proton.engine.impl.ssl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import org.apache.qpid.proton.engine.SslDomain;
import org.apache.qpid.proton.engine.SslDomain.Mode;
import org.apache.qpid.proton.engine.SslDomain.VerifyMode;
import org.junit.Test;

public class SslDomainImplTest {

    @Test
    public void testInitGetMode() throws Exception
    {
        SslDomain sslDomain = SslDomain.Factory.create();
        assertNull("Unexpected mode, none was set", sslDomain.getMode());

        sslDomain.init(Mode.CLIENT);
        assertEquals("Unexpected mode", Mode.CLIENT, sslDomain.getMode());

        sslDomain.init(Mode.SERVER);
        assertEquals("Unexpected mode", Mode.SERVER, sslDomain.getMode());
    }

    @Test
    public void testVerifyModeDefault() throws Exception
    {
        SslDomain clientSslDomain = SslDomain.Factory.create();
        assertEquals("Unexpected default verification mode", VerifyMode.VERIFY_PEER_NAME, clientSslDomain.getPeerAuthentication());
        clientSslDomain.init(Mode.CLIENT);
        assertEquals("Unexpected default verification mode", VerifyMode.VERIFY_PEER_NAME, clientSslDomain.getPeerAuthentication());

        SslDomain serverSslDomain = SslDomain.Factory.create();
        serverSslDomain.init(Mode.SERVER);
        assertEquals("Unexpected default verification mode", VerifyMode.ANONYMOUS_PEER, serverSslDomain.getPeerAuthentication());
    }

    @Test
    public void testVerifyModeSetGet() throws Exception
    {
        SslDomain clientSslDomain = SslDomain.Factory.create();
        clientSslDomain.init(Mode.CLIENT);
        assertNotEquals("Unexpected verification mode", VerifyMode.VERIFY_PEER, clientSslDomain.getPeerAuthentication());
        clientSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER);
        assertEquals("Unexpected verification mode", VerifyMode.VERIFY_PEER, clientSslDomain.getPeerAuthentication());

        SslDomain serverSslDomain = SslDomain.Factory.create();
        serverSslDomain.init(Mode.SERVER);
        assertNotEquals("Unexpected verification mode", VerifyMode.VERIFY_PEER, serverSslDomain.getPeerAuthentication());
        serverSslDomain.setPeerAuthentication(VerifyMode.VERIFY_PEER);
        assertEquals("Unexpected verification mode", VerifyMode.VERIFY_PEER, serverSslDomain.getPeerAuthentication());
    }
}
