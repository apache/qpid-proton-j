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

import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.junit.Test;

/**
 * Test for decoder of AMQP Header type.
 */
public class HeaderTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024 * 10;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeHeader() throws IOException {
        doTestDecodeHeaderSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfHeaders() throws IOException {
        doTestDecodeHeaderSeries(LARGE_SIZE);
    }

    private void doTestDecodeHeaderSeries(int size) throws IOException {
        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority(UnsignedByte.valueOf((byte) 3));
        header.setDeliveryCount(UnsignedInteger.valueOf(10));
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTtl(UnsignedInteger.valueOf(500));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(header);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Header);

            Header decoded = (Header) result;

            assertEquals(3, decoded.getPriority().intValue());
            assertTrue(decoded.getDurable().booleanValue());
        }
    }

    @Test
    public void testSkipHeader() throws IOException {
        Header header1 = new Header();
        Header header2 = new Header();

        header1.setDurable(Boolean.FALSE);
        header2.setDurable(Boolean.TRUE);

        encoder.writeObject(header1);
        encoder.writeObject(header2);

        buffer.clear();

        TypeConstructor<?> headerType = decoder.readConstructor();
        assertEquals(Header.class, headerType.getTypeClass());
        headerType.skipValue();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof Header);

        Header decoded = (Header) result;
        assertTrue(decoded.getDurable().booleanValue());
    }

    @Test
    public void testDecodeHeaderArray() throws IOException {
        Header header = new Header();

        header.setDurable(Boolean.TRUE);
        header.setPriority(UnsignedByte.valueOf((byte) 3));
        header.setDeliveryCount(UnsignedInteger.valueOf(10));
        header.setFirstAcquirer(Boolean.FALSE);
        header.setTtl(UnsignedInteger.valueOf(500));

        Header[] source = new Header[32];

        for (int i = 0; i < source.length; ++i) {
            source[i] = header;
        }

        encoder.writeObject(source);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result.getClass().isArray());

        final Object[] resultArray = (Object[]) result;

        for (int i = 0; i < source.length; ++i) {

            assertTrue(resultArray[i] instanceof Header);

            Header decoded = (Header) resultArray[i];

            assertEquals(3, decoded.getPriority().intValue());
            assertTrue(decoded.getDurable().booleanValue());
            assertEquals(header.getDeliveryCount(), decoded.getDeliveryCount());
        }
    }
}
