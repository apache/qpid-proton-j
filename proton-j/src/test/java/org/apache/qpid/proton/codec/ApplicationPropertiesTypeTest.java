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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.junit.Test;
import org.mockito.Mockito;

public class ApplicationPropertiesTypeTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeSmallSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfApplicationProperties() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        long currentTime = System.currentTimeMillis();

        propertiesMap.put("long-1", currentTime);
        propertiesMap.put("date-1", new Date(currentTime));
        propertiesMap.put("long-2", currentTime + 100);
        propertiesMap.put("date-2", new Date(currentTime + 100));
        propertiesMap.put("key-1", "1");
        propertiesMap.put("key-2", "2");
        propertiesMap.put("key-3", "3");
        propertiesMap.put("key-4", "4");
        propertiesMap.put("key-5", "5");
        propertiesMap.put("key-6", "6");
        propertiesMap.put("key-7", "7");
        propertiesMap.put("key-8", "8");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(properties);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof ApplicationProperties);

            ApplicationProperties decoded = (ApplicationProperties) result;

            assertEquals(properties.getValue().size(), decoded.getValue().size());
            assertTrue(decoded.getValue().equals(propertiesMap));

            assertEquals(currentTime, decoded.getValue().get("long-1"));
            assertEquals(new Date(currentTime), decoded.getValue().get("date-1"));
            assertEquals(currentTime + 100, decoded.getValue().get("long-2"));
            assertEquals(new Date(currentTime + 100), decoded.getValue().get("date-2"));
        }
    }

    @Test
    public void testEncodeApplicationPropertiesReservesSpaceForPayload() throws IOException {
        final int ENTRIES = 8;

        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        ApplicationProperties properties = new ApplicationProperties(propertiesMap);

        for (int i = 0; i < ENTRIES; ++i) {
            properties.getValue().put(String.valueOf(i), i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeObject(properties);

        // Check that the Type tries to reserve space, actual encoding size not computed here.
        // Each key should also try and reserve space for the String data
        Mockito.verify(spy, Mockito.times(ENTRIES + 1)).ensureRemaining(Mockito.anyInt());
    }
}
