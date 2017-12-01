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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.proton.amqp.DescribedType;
import org.junit.Test;

/**
 * Tests the handling of UnknownDescribedType instances.
 */
public class UnknownDescribedTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeUnknownDescribedType() throws Exception {
        encoder.writeObject(NoLocalType.NO_LOCAL);

        buffer.clear();

        Object result = decoder.readObject();
        assertTrue(result instanceof DescribedType);
        DescribedType resultTye = (DescribedType) result;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @Test
    public void testDecodeSmallSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfUnknownDescribedTypes() throws IOException {
        doTestDecodeUnknownDescribedTypeSeries(LARGE_SIZE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnknownDescribedTypeInList() throws IOException {
        List<Object> listOfUnkowns = new ArrayList<>();

        listOfUnkowns.add(NoLocalType.NO_LOCAL);

        encoder.writeList(listOfUnkowns);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof List);

        final List<Object> decodedList = (List<Object>) result;
        assertEquals(1, decodedList.size());

        final Object listEntry = decodedList.get(0);
        assertTrue(listEntry instanceof DescribedType);

        DescribedType resultTye = (DescribedType) listEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnknownDescribedTypeInMap() throws IOException {
        Map<Object, Object> mapOfUnknowns = new HashMap<>();

        mapOfUnknowns.put(NoLocalType.NO_LOCAL.getDescriptor(), NoLocalType.NO_LOCAL);

        encoder.writeMap(mapOfUnknowns);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof Map);

        final Map<Object, Object> decodedMap = (Map<Object, Object>) result;
        assertEquals(1, decodedMap.size());

        final Object mapEntry = decodedMap.get(NoLocalType.NO_LOCAL.getDescriptor());
        assertTrue(mapEntry instanceof DescribedType);

        DescribedType resultTye = (DescribedType) mapEntry;
        assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
    }

    private void doTestDecodeUnknownDescribedTypeSeries(int size) throws IOException {
        for (int i = 0; i < size; ++i) {
            encoder.writeObject(NoLocalType.NO_LOCAL);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof DescribedType);

            DescribedType resultTye = (DescribedType) result;
            assertEquals(NoLocalType.NO_LOCAL.getDescriptor(), resultTye.getDescriptor());
        }
    }
}
