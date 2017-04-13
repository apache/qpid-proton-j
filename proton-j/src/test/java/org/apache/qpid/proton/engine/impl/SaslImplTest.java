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

import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton.amqp.Binary;
import org.junit.Test;

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

}
