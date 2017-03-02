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
package org.apache.qpid.proton.amqp.messaging;

import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

/**
 *
 */
public class PropertiesTest {

    @Test
    public void testCopy() {
        Properties original = new Properties();

        original.setAbsoluteExpiryTime(new Date(System.currentTimeMillis()));
        original.setContentEncoding(Symbol.valueOf("utf-8"));
        original.setContentType(Symbol.valueOf("test/plain"));
        original.setCorrelationId("1");
        original.setCreationTime(new Date(System.currentTimeMillis()));
        original.setGroupId("group-1");
        original.setGroupSequence(UnsignedInteger.MAX_VALUE);
        original.setMessageId("ID:1");
        original.setReplyTo("queue");
        original.setReplyToGroupId("3");
        original.setSubject("subject");
        original.setTo("to-queue");
        original.setUserId(new Binary(new byte[1]));

        Properties copy = new Properties(original);

        assertEquals(original.getAbsoluteExpiryTime(), copy.getAbsoluteExpiryTime());
        assertEquals(original.getContentEncoding(), copy.getContentEncoding());
        assertEquals(original.getContentType(), copy.getContentType());
        assertEquals(original.getCorrelationId(), copy.getCorrelationId());
        assertEquals(original.getCreationTime(), copy.getCreationTime());
        assertEquals(original.getGroupId(), copy.getGroupId());
        assertEquals(original.getGroupSequence(), copy.getGroupSequence());
        assertEquals(original.getMessageId(), copy.getMessageId());
        assertEquals(original.getReplyTo(), copy.getReplyTo());
        assertEquals(original.getReplyToGroupId(), copy.getReplyToGroupId());
        assertEquals(original.getSubject(), copy.getSubject());
        assertEquals(original.getTo(), copy.getTo());
        assertEquals(original.getUserId(), copy.getUserId());
    }

}
