/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton.codec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.junit.Test;

/**
 * Test for decoder of AMQP Properties type.
 */
public class PropertiesCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodePropertiess() throws IOException {
        doTestDecodePropertiesSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfPropertiess() throws IOException {
        doTestDecodePropertiesSeries(LARGE_SIZE);
    }

    private void doTestDecodePropertiesSeries(int size) throws IOException {
        Properties properties = new Properties();

        Date timeNow = new Date(System.currentTimeMillis());

        properties.setMessageId("ID:Message-1:1:1:0");
        properties.setUserId(new Binary(new byte[1]));
        properties.setTo("queue:work");
        properties.setSubject("help");
        properties.setReplyTo("queue:temp:me");
        properties.setContentEncoding(Symbol.valueOf("text/UTF-8"));
        properties.setContentType(Symbol.valueOf("text"));
        properties.setCorrelationId("correlation-id");
        properties.setAbsoluteExpiryTime(timeNow);
        properties.setCreationTime(timeNow);
        properties.setGroupId("group-1");
        properties.setGroupSequence(UnsignedInteger.valueOf(1));
        properties.setReplyToGroupId("group-1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(properties);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Properties);

            Properties decoded = (Properties) result;

            assertNotNull(decoded.getAbsoluteExpiryTime());
            assertEquals(timeNow, decoded.getAbsoluteExpiryTime());
            assertEquals(Symbol.valueOf("text/UTF-8"), decoded.getContentEncoding());
            assertEquals(Symbol.valueOf("text"), decoded.getContentType());
            assertEquals("correlation-id", decoded.getCorrelationId());
            assertEquals(timeNow, decoded.getCreationTime());
            assertEquals("group-1", decoded.getGroupId());
            assertEquals(UnsignedInteger.valueOf(1), decoded.getGroupSequence());
            assertEquals("ID:Message-1:1:1:0", decoded.getMessageId());
            assertEquals("queue:temp:me", decoded.getReplyTo());
            assertEquals("group-1", decoded.getReplyToGroupId());
            assertEquals("help", decoded.getSubject());
            assertEquals("queue:work", decoded.getTo());
            assertTrue(decoded.getUserId() instanceof Binary);
        }
    }
}
