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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.junit.Test;
import org.mockito.Mockito;

public class FooterCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testDecodeSmallSeriesOfFooter() throws IOException {
        doTestDecodeFooterSeries(SMALL_SIZE);
    }

    @Test
    public void testDecodeLargeSeriesOfFooter() throws IOException {
        doTestDecodeFooterSeries(LARGE_SIZE);
    }

    private void doTestDecodeFooterSeries(int size) throws IOException {

        Map<Symbol, Object> footerMap = new LinkedHashMap<>();
        Footer properties = new Footer(footerMap);

        footerMap.put(Symbol.valueOf("3"), "3");
        footerMap.put(Symbol.valueOf("2"), "2");
        footerMap.put(Symbol.valueOf("1"), "1");

        for (int i = 0; i < size; ++i) {
            encoder.writeObject(properties);
        }

        buffer.clear();

        for (int i = 0; i < size; ++i) {
            final Object result = decoder.readObject();

            assertNotNull(result);
            assertTrue(result instanceof Footer);

            Footer decoded = (Footer) result;

            assertEquals(properties.getValue().size(), decoded.getValue().size());
            assertTrue(decoded.getValue().equals(footerMap));
        }
    }

    @Test
    public void testEncodeFooterReservesSpaceForPayload() throws IOException {
        final int ENTRIES = 8;

        Map<Symbol, Object> footerMap = new LinkedHashMap<>();
        for (int i = 0; i < ENTRIES; ++i) {
            footerMap.put(Symbol.valueOf(String.valueOf(i)), i);
        }

        Footer footer = new Footer(footerMap);

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeObject(footer);

        // Check that the Type tries to reserve space, actual encoding size not computed here.
        // Each key should also try and reserve space for the String data
        Mockito.verify(spy, Mockito.times(ENTRIES + 1)).ensureRemaining(Mockito.anyInt());
    }

      @Test
      public void testDecodeFooter() throws Exception {
          Object o = doDecodeFooterWithSpecificSizeCountTestImpl(false, 25, 6);
          assertDecodedFooter(o);

          o = doDecodeFooterWithSpecificSizeCountTestImpl(true, 28, 6);
          assertDecodedFooter(o);
      }

      @Test
      public void testDecodeThrowsWithSizeBeyondAvailable() throws Exception {
          try {
              doDecodeFooterWithSpecificSizeCountTestImpl(false, 25 + 1, 6);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map size 26 cannot be more than remaining bytes 25", de.getMessage());
          }

          try {
              doDecodeFooterWithSpecificSizeCountTestImpl(true, 28 + 1, 6);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map size 29 cannot be more than remaining bytes 28", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithNegativeSize() throws Exception {
          try {
              doDecodeFooterWithSpecificSizeCountTestImpl(true, UnsignedInteger.MAX_VALUE.intValue(), 6);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map size -1 cannot be negative", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithNegativeCount() throws Exception {
          try {
              doDecodeFooterWithSpecificSizeCountTestImpl(true, 28, UnsignedInteger.MAX_VALUE.intValue());
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count -1 cannot be negative", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithCountLargerThanSize() throws Exception {
          try {
              // Deliberately tries not to decode the full map, so the 'remaining' checks dont foul
              doDecodeFooterWithSpecificSizeCountTestImpl(true, 20, 22);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count 22 must be less than size (20)", de.getMessage());
          }
      }

      @Test
      public void testDecodeThrowsWithOddCount() throws Exception {
          try {
              doDecodeFooterWithSpecificSizeCountTestImpl(false, 25, 5);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count 5 cannot be odd", de.getMessage());
          }

          try {
              doDecodeFooterWithSpecificSizeCountTestImpl(true, 28, 5);
              fail("Expected exception to be thrown");
          } catch (DecodeException de) {
              assertEquals("Map element count 5 cannot be odd", de.getMessage());
          }
      }

      private Object doDecodeFooterWithSpecificSizeCountTestImpl(final boolean map32, int size, int count) {
          ByteBuffer tmpBuf = ByteBuffer.allocate(1024);
          tmpBuf.put((byte) 0x00); // DescribedType
          tmpBuf.put((byte) 0x53); // small-ulong
          tmpBuf.put((byte) 0x78); // footer ulong descriptor
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

          decoder.setByteBuffer(tmpBuf);

          return decoder.readObject();
      }

      private void assertDecodedFooter(Object o) {
          assertTrue(o instanceof Footer);
          @SuppressWarnings("unchecked")
          Map<Symbol,Object> map = ((Footer) o).getValue();
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

}
