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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.qpid.proton.amqp.Symbol;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the encoding and decoding of {@link SymbolType} values.
 */
public class SymbolTypeTest extends CodecTestSupport {

    private final int LARGE_SIZE = 512;
    private final int SMALL_SIZE = 32;

    @Test
    public void testEncodeSmallSymbolReservesSpaceForPayload() throws IOException {
        doTestEncodeSymbolTypeReservation(SMALL_SIZE);
    }

    @Test
    public void testEncodeLargeSymbolReservesSpaceForPayload() throws IOException {
        doTestEncodeSymbolTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeSymbolTypeReservation(int size) throws IOException {
        Random random = new Random(System.currentTimeMillis());
        StringBuilder builder = new StringBuilder(size);
        for (int i = 0; i < size; ++i) {
            builder.append((byte) random.nextInt(127));
        }

        Symbol symbol = Symbol.valueOf(builder.toString());

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(ByteBuffer.allocate(2048));
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeSymbol(symbol);

        // Check that the SymbolType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }
}
