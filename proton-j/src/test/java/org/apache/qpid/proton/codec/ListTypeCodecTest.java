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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for the Proton List encoder / decoder
 */
public class ListTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeList() throws IOException {
        doTestDecodeListSeries(1);
    }

    @Test
    public void testDecodeSmallSeriesOfLists() throws IOException {
        doTestDecodeListSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfLists() throws IOException {
        doTestDecodeListSeries(LARGE_SIZE);
    }

    @Test
    public void testDecodeSmallSeriesOfSymbolLists() throws IOException {
        doTestDecodeSymbolListSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfSymbolLists() throws IOException {
        doTestDecodeSymbolListSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeSymbolListSeries(int size) throws IOException {
        List<Object> list = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            list.add(Symbol.valueOf(String.valueOf(i)));
        }

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(list);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof List);

            List<Object> resultList = (List<Object>) result;

            assertEquals(list.size(), resultList.size());
        }
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeListSeries(int size) throws IOException {
        List<Object> list = new ArrayList<>();

        Date timeNow = new Date(System.currentTimeMillis());

        list.add("ID:Message-1:1:1:0");
        list.add(new Binary(new byte[1]));
        list.add("queue:work");
        list.add(Symbol.valueOf("text/UTF-8"));
        list.add(Symbol.valueOf("text"));
        list.add(timeNow);
        list.add(UnsignedInteger.valueOf(1));
        list.add(UUID.randomUUID());

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(list);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof List);

            List<Object> resultList = (List<Object>) result;

            assertEquals(list.size(), resultList.size());
        }
    }

    @Test
    public void testEncodeSmallListReservesSpaceForPayload() throws IOException {
        doTestEncodeListTypeReservation(SMALL_SIZE);
    }

    @Test
    public void testEncodeLargeListReservesSpaceForPayload() throws IOException {
        doTestEncodeListTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeListTypeReservation(int size) throws IOException {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            list.add(i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeList(list);

        // Check that the ListType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testEncodeListWithUnknownEntryType() throws Exception {
        List<Object> list = new ArrayList<>();
        list.add(new MyUnknownTestType());

        doTestEncodeListWithUnknownEntryTypeTestImpl(list);
    }

    @Test
    public void testEncodeSubListWithUnknownEntryType() throws Exception {
        List<Object> subList = new ArrayList<>();
        subList.add(new MyUnknownTestType());

        List<Object> list = new ArrayList<>();
        list.add(subList);

        doTestEncodeListWithUnknownEntryTypeTestImpl(list);
    }

    private void doTestEncodeListWithUnknownEntryTypeTestImpl(List<Object> list) {
        try {
            encoder.writeList(list);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("No encoding defined for type: class org.apache.qpid.proton.codec.MyUnknownTestType"));
        }
    }
}
