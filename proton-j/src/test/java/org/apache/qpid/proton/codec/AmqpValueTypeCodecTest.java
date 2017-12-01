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
import java.util.UUID;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.junit.Test;

/**
 * Test for decoder of the AmqpValue type.
 */
public class AmqpValueTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeAmqpValueString() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue("test"));
    }

    @Test
    public void testDecodeAmqpValueNull() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue(null));
    }

    @Test
    public void testDecodeAmqpValueUUID() throws IOException {
        doTestDecodeAmqpValueSeries(1, new AmqpValue(UUID.randomUUID()));
    }

    @Test
    public void testDecodeSmallSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(SMALL_SIZE, new AmqpValue("test"));
    }

    @Test
    public void testDecodeLargeSeriesOfAmqpValue() throws IOException {
        doTestDecodeAmqpValueSeries(LARGE_SIZE, new AmqpValue("test"));
    }

    private void doTestDecodeAmqpValueSeries(int size, AmqpValue value) throws IOException {

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(value);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof AmqpValue);

            AmqpValue decoded = (AmqpValue) result;

            assertEquals(value.getValue(), decoded.getValue());
        }
    }
}
