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
package org.apache.qpid.proton.amqp.transport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.junit.Test;

public class BeginTest {

    @Test
    public void testCopy() {
        Map<Symbol, Object> properties = new HashMap<>();
        properties.put(Symbol.valueOf("x-opt"), "value");

        Begin begin = new Begin();
        begin.setRemoteChannel(UnsignedShort.valueOf((short) 2));
        begin.setNextOutgoingId(UnsignedInteger.valueOf(10));
        begin.setIncomingWindow(UnsignedInteger.valueOf(11));
        begin.setOutgoingWindow(UnsignedInteger.valueOf(12));
        begin.setHandleMax(UnsignedInteger.valueOf(13));
        begin.setDesiredCapabilities(new Symbol[0]);
        begin.setOfferedCapabilities(new Symbol[] { Symbol.valueOf("anonymous-relay") });
        begin.setProperties(properties);

        final Begin copyOf = begin.copy();

        assertEquals(begin.getRemoteChannel(), copyOf.getRemoteChannel());
        assertEquals(begin.getNextOutgoingId(), copyOf.getNextOutgoingId());
        assertEquals(begin.getIncomingWindow(), copyOf.getIncomingWindow());
        assertEquals(begin.getOutgoingWindow(), copyOf.getOutgoingWindow());
        assertEquals(begin.getHandleMax(), copyOf.getHandleMax());
        assertArrayEquals(begin.getDesiredCapabilities(), copyOf.getDesiredCapabilities());
        assertArrayEquals(begin.getOfferedCapabilities(), copyOf.getOfferedCapabilities());
        assertEquals(begin.getProperties(), copyOf.getProperties());
    }
}
