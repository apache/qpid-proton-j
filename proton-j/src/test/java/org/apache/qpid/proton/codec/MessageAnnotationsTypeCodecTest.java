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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.junit.Test;

public class MessageAnnotationsTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeSmallSeriesOfMessageAnnotations() throws IOException {
        doTestDecodeMessageAnnotationsSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfMessageAnnotations() throws IOException {
        doTestDecodeMessageAnnotationsSeries(LARGE_SIZE);
    }

    @Test
    public void testDecodeLMessageAnnotations() throws IOException {
        doTestDecodeMessageAnnotationsSeries(1);
    }

    private void doTestDecodeMessageAnnotationsSeries(int size) throws IOException {

        final Symbol SYMBOL_1 = Symbol.valueOf("test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("test2");
        final Symbol SYMBOL_3 = Symbol.valueOf("test3");

        MessageAnnotations annotations = new MessageAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, UnsignedByte.valueOf((byte) 128));
        annotations.getValue().put(SYMBOL_2, UnsignedShort.valueOf((short) 128));
        annotations.getValue().put(SYMBOL_3, UnsignedInteger.valueOf(128));

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(annotations);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof MessageAnnotations);

            MessageAnnotations readAnnotations = (MessageAnnotations) result;

            Map<Symbol, Object> resultMap = readAnnotations.getValue();

            assertEquals(annotations.getValue().size(), resultMap.size());
            assertEquals(resultMap.get(SYMBOL_1), UnsignedByte.valueOf((byte) 128));
            assertEquals(resultMap.get(SYMBOL_2), UnsignedShort.valueOf((short) 128));
            assertEquals(resultMap.get(SYMBOL_3), UnsignedInteger.valueOf(128));
        }
    }

    @Test
    public void testEncodeAndDecodeAnnoationsWithEmbeddedMaps() throws IOException {
        final Symbol SYMBOL_1 = Symbol.valueOf("x-opt-test1");
        final Symbol SYMBOL_2 = Symbol.valueOf("x-opt-test2");

        final String VALUE_1 = "string";
        final UnsignedInteger VALUE_2 = UnsignedInteger.valueOf(42);
        final UUID VALUE_3 = UUID.randomUUID();

        Map<String, Object> stringKeyedMap = new HashMap<>();
        stringKeyedMap.put("key1", VALUE_1);
        stringKeyedMap.put("key2", VALUE_2);
        stringKeyedMap.put("key3", VALUE_3);

        Map<Symbol, Object> symbolKeyedMap = new HashMap<>();
        symbolKeyedMap.put(Symbol.valueOf("key1"), VALUE_1);
        symbolKeyedMap.put(Symbol.valueOf("key2"), VALUE_2);
        symbolKeyedMap.put(Symbol.valueOf("key3"), VALUE_3);

        MessageAnnotations annotations = new MessageAnnotations(new HashMap<>());
        annotations.getValue().put(SYMBOL_1, stringKeyedMap);
        annotations.getValue().put(SYMBOL_2, symbolKeyedMap);

        encoder.writeObject(annotations);

        buffer.clear();

        final Object result = decoder.readObject();

        assertNotNull(result);
        assertTrue(result instanceof MessageAnnotations);

        MessageAnnotations readAnnotations = (MessageAnnotations) result;

        Map<Symbol, Object> resultMap = readAnnotations.getValue();

        assertEquals(annotations.getValue().size(), resultMap.size());
        assertEquals(resultMap.get(SYMBOL_1), stringKeyedMap);
        assertEquals(resultMap.get(SYMBOL_2), symbolKeyedMap);
    }

      @Test
      public void testDecodeMessageAnnotations() throws Exception {
          Object o = doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(false, 25, 6);
          assertDecodedMessageAnnotations(o);

          o = doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 28, 6);
          assertDecodedMessageAnnotations(o);
      }

      @Test
      public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
          try {
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(false, 25 + 1, 6);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map size 26 cannot be more than remaining bytes 25", de.getMessage());
          }

          try {
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 28 + 1, 6);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map size 29 cannot be more than remaining bytes 28", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithNegativeSize() throws Exception {
          try {
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map size -1 cannot be negative", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithNegativeCount() throws Exception {
          try {
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 28, UnsignedInteger.MAX_VALUE.intValue());
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count -1 cannot be negative", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
          try {
              // Deliberately tries not to decode the full map, so the 'remaining' checks dont foul
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 20, 22);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count 22 must be less than size (20)", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithOddCount() throws Exception {
          try {
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(false, 25, 5);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count 5 cannot be odd", de.getMessage());
          }

          try {
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 28, 5);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count 5 cannot be odd", de.getMessage());
          }
      }

      private Object doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(final boolean map32, int size, int count) {
          ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
          tmpBuf.put((byte) 0x00); // DescribedType
          tmpBuf.put((byte) 0x53); // small-ulong
          tmpBuf.put((byte) 0x72); // message-annotations ulong descriptor
          if (map32) {
              tmpBuf.put((byte) (byte) 0xD1); // map32 primitive type constructor
              tmpBuf.putInt(size); // size
              tmpBuf.putInt(count); // count
          } else {
              tmpBuf.put((byte) (byte) 0xC1); // map8 primitive type constructor
              tmpBuf.put((byte) size); // size
              tmpBuf.put((byte) count); // count
          }

          tmpBuf.put((byte) 0xA3); // sym8
          tmpBuf.put((byte) 1); // length
          tmpBuf.put((byte) 0x33); // "3"
          tmpBuf.put((byte) 0x71); // int
          tmpBuf.putInt(3);

          tmpBuf.put((byte) 0xA3); // sym8
          tmpBuf.put((byte) 1); // length
          tmpBuf.put((byte) 0x32); // "2"
          tmpBuf.put((byte) 0x71); // int
          tmpBuf.putInt(2);

          tmpBuf.put((byte) 0xA3); // sym8
          tmpBuf.put((byte) 1); // length
          tmpBuf.put((byte) 0x31); // "1"
          tmpBuf.put((byte) 0x71); // int
          tmpBuf.putInt(1);

          tmpBuf.flip();
          int oldLimit = tmpBuf.limit();

          decoder.setByteBuffer(tmpBuf);

          final Object decoded;
          try {
              decoded = decoder.readObject();
          } finally {
              assertEquals(oldLimit, tmpBuf.limit());
          }
          assertEquals(oldLimit, tmpBuf.position());

          return decoded;
      }

      private void assertDecodedMessageAnnotations(Object o) {
          assertTrue(o instanceof MessageAnnotations);
          Map<Symbol,Object> map = ((MessageAnnotations)o).getValue();
          assertTrue(map instanceof LinkedHashMap);

          assertEquals(3, map.size());
          assertEquals(Integer.valueOf(3), map.get(Symbol.valueOf("3")));
          assertEquals(Integer.valueOf(2), map.get(Symbol.valueOf("2")));
          assertEquals(Integer.valueOf(1), map.get(Symbol.valueOf("1")));

          Iterator<Map.Entry<Symbol,Object>> iterator = map.entrySet().iterator();
          assertTrue(iterator.hasNext());

          Entry<Symbol, Object> entry = iterator.next();
          assertEquals(Symbol.valueOf("3"), entry.getKey());
          assertEquals(Integer.valueOf(3), entry.getValue());

          assertTrue(iterator.hasNext());
          entry = iterator.next();
          assertEquals(Symbol.valueOf("2"), entry.getKey());
          assertEquals(Integer.valueOf(2), entry.getValue());

          assertTrue(iterator.hasNext());
          entry = iterator.next();
          assertEquals(Symbol.valueOf("1"), entry.getKey());
          assertEquals(Integer.valueOf(1), entry.getValue());
      }

      @Test
      public void testDecodeOverflowThrows() throws Exception {
          try {
              // Deliberately pass size for part map, but count for full map...will try to decode count, overflow size
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(false, 25 -8, 6);
              fail("Expected exception to be thrown");
          } catch (BufferUnderflowException bue) {
              // Expected
          }

          try {
              // Deliberately pass size for part map, but count for full map...will try to decode count, overflow size
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 28 - 8, 6);
              fail("Expected exception to be thrown");
          } catch (BufferUnderflowException bue) {
              // Expected
          }
      }

      @Test
      public void testDecodeUnderflowThrows() throws Exception {
          try {
              // Deliberately pass size for full map, but count for part map...will try to decode count, underflow size
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(false, 25, 6 - 2);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("MessageAnnotations incorrectly encoded, left 8 bytes from size indicated 25", de.getMessage());
          }

          try {
              // Deliberately pass size for full map, but count for part map...will try to decode count, underflow size
              doDecodeMessageAnnotationsWithSpecificSizeCountTestImpl(true, 28, 6 - 2);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("MessageAnnotations incorrectly encoded, left 8 bytes from size indicated 28", de.getMessage());
          }
      }
}
