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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.junit.Test;
import org.mockito.Mockito;

public class MapTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeSmallSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfMaps() throws IOException {
        doTestDecodeMapSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    private void doTestDecodeMapSeries(int size) throws IOException {

        String myBoolKey = "myBool";
        boolean myBool = true;
        String myByteKey = "myByte";
        byte myByte = 4;
        String myBytesKey = "myBytes";
        byte[] myBytes = myBytesKey.getBytes();
        String myCharKey = "myChar";
        char myChar = 'd';
        String myDoubleKey = "myDouble";
        double myDouble = 1234567890123456789.1234;
        String myFloatKey = "myFloat";
        float myFloat = 1.1F;
        String myIntKey = "myInt";
        int myInt = Integer.MAX_VALUE;
        String myLongKey = "myLong";
        long myLong = Long.MAX_VALUE;
        String myShortKey = "myShort";
        short myShort = 25;
        String myStringKey = "myString";
        String myString = myStringKey;

        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put(myBoolKey, myBool);
        map.put(myByteKey, myByte);
        map.put(myBytesKey, new Binary(myBytes));
        map.put(myCharKey, myChar);
        map.put(myDoubleKey, myDouble);
        map.put(myFloatKey, myFloat);
        map.put(myIntKey, myInt);
        map.put(myLongKey, myLong);
        map.put(myShortKey, myShort);
        map.put(myStringKey, myString);

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(map);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Map);

            Map<String, Object> resultMap = (Map<String, Object>) result;

            assertEquals(map.size(), resultMap.size());
        }
    }

    @Test
    public void testEncodeSmallMapReservesSpaceForPayload() throws IOException {
        doTestEncodeMapTypeReservation(SMALL_SIZE);
    }

    @Test
    public void doTestEncodeMapTypeReservation() throws IOException {
        doTestEncodeMapTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeMapTypeReservation(int size) throws IOException {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < size; ++i) {
            map.put(i, i);
        }

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeMap(map);

        // Check that the MapType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }

    @Test
    public void testEncodeMapWithUnknownEntryValueType() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("unknown", new MyUnknownTestType());

        doTestEncodeMapWithUnknownEntryValueTypeTestImpl(map);
    }

    @Test
    public void testEncodeSubMapWithUnknownEntryValueType() throws Exception {
        Map<String, Object> subMap = new HashMap<>();
        subMap.put("unknown", new MyUnknownTestType());

        Map<String, Object> map = new HashMap<>();
        map.put("submap", subMap);

        doTestEncodeMapWithUnknownEntryValueTypeTestImpl(map);
    }

    private void doTestEncodeMapWithUnknownEntryValueTypeTestImpl(Map<String, Object> map) {
        try {
            encoder.writeMap(map);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("No encoding is known for map entry value of type: org.apache.qpid.proton.codec.MyUnknownTestType"));
        }
    }

    @Test
    public void testEncodeMapWithUnknownEntryKeyType() throws Exception {
        Map<Object, String> map = new HashMap<>();
        map.put(new MyUnknownTestType(), "unknown");

        doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(map);
    }

    @Test
    public void testEncodeSubMapWithUnknownEntryKeyType() throws Exception {
        Map<Object, String> subMap = new HashMap<>();
        subMap.put(new MyUnknownTestType(), "unknown");

        Map<String, Object> map = new HashMap<>();
        map.put("submap", subMap);

        doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(map);
    }

    private void doTestEncodeMapWithUnknownEntryKeyTypeTestImpl(Map<?, ?> map) {
        try {
            encoder.writeMap(map);
            fail("Expected exception to be thrown");
        } catch (IllegalArgumentException iae) {
            assertThat(iae.getMessage(), containsString("No encoding is known for map entry key of type: org.apache.qpid.proton.codec.MyUnknownTestType"));
        }
    }
}
