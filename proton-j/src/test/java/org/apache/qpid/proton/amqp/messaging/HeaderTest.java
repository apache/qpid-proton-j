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

import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;

public class HeaderTest {

    @Test
    public void testCopy() {
        Header original = new Header();

        original.setDeliveryCount(UnsignedInteger.valueOf(1));
        original.setDurable(true);
        original.setFirstAcquirer(true);
        original.setPriority(UnsignedByte.valueOf((byte) 7));
        original.setTtl(UnsignedInteger.valueOf(100));

        Header copy = new Header(original);

        assertEquals(original.getDeliveryCount(), copy.getDeliveryCount());
        assertEquals(original.getDurable(), copy.getDurable());
        assertEquals(original.getFirstAcquirer(), copy.getFirstAcquirer());
        assertEquals(original.getPriority(), copy.getPriority());
        assertEquals(original.getTtl(), copy.getTtl());
    }
}
