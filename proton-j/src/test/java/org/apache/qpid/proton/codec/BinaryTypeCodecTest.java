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

import org.apache.qpid.proton.amqp.Binary;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for the Proton Binary type encoder / decoder
 */
public class BinaryTypeCodecTest extends CodecTestSupport {

    private final int LARGE_SIZE = 1024;
    private final int SMALL_SIZE = 32;

    @Test
    public void testEncodeSmallBinaryReservesSpaceForPayload() throws IOException {
        doTestEncodeBinaryTypeReservation(SMALL_SIZE);
    }

    @Test
    public void testEncodeLargeBinaryReservesSpaceForPayload() throws IOException {
        doTestEncodeBinaryTypeReservation(LARGE_SIZE);
    }

    private void doTestEncodeBinaryTypeReservation(int size) throws IOException {
        byte[] data = new byte[size];
        for (int i = 0; i < size; ++i) {
            data[i] = (byte) (i % 255);
        }

        Binary binary = new Binary(data);

        WritableBuffer writable = new WritableBuffer.ByteBufferWrapper(this.buffer);
        WritableBuffer spy = Mockito.spy(writable);

        encoder.setByteBuffer(spy);
        encoder.writeBinary(binary);

        // Check that the BinaryType tries to reserve space, actual encoding size not computed here.
        Mockito.verify(spy).ensureRemaining(Mockito.anyInt());
    }
}
