/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.proton.codec;

import java.nio.ByteBuffer;

public class Hashing {

    private Hashing() {

    }

    // this const propagation should be already handled by the JIT
    // but we do this to make it more readable
    private static final int PRIME_1 = 31;
    private static final int PRIME_2 = PRIME_1 * PRIME_1;
    private static final int PRIME_3 = PRIME_2 * PRIME_1;
    private static final int PRIME_4 = PRIME_3 * PRIME_1;
    private static final int PRIME_5 = PRIME_4 * PRIME_1;
    private static final int PRIME_6 = PRIME_5 * PRIME_1;
    private static final int PRIME_7 = PRIME_6 * PRIME_1;
    private static final int PRIME_8 = PRIME_7 * PRIME_1;

    public static int byteBufferCompatibleHashCode(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            final int arrayOffset = byteBuffer.arrayOffset();
            final int arrayPosition = arrayOffset + byteBuffer.position();
            final int arrayLimit = arrayOffset + byteBuffer.limit();
            return byteBufferCompatibleHashCode(byteBuffer.array(), arrayPosition, arrayLimit);
        }
        // direct ByteBuffers does have some heavy-weight bound checks and memory barriers that
        // we just hope JIT to be better then us!
        return byteBuffer.hashCode();
    }

    public static int byteBufferCompatibleHashCode(byte[] bytes, int position, int limit) {
        int h = 1;
        int remaining = limit - position;
        if (remaining == 0) {
            return h;
        }
        int index = limit - 1;
        // unrolled version
        final int bytesCount = remaining & 7;
        if (bytesCount > 0) {
            assert h == 1;
            h = unrolledHashCode(bytes, index, bytesCount, 1);
            index -= bytesCount;
        }
        final long longsCount = remaining >>> 3;
        // let's break the data dependency of each per element hash code
        // and save bound checks by manual unrolling 8 ops at time
        for (int i = 0; i < longsCount; i++) {
            final byte b7 = bytes[index];
            final byte b6 = bytes[index - 1];
            final byte b5 = bytes[index - 2];
            final byte b4 = bytes[index - 3];
            final byte b3 = bytes[index - 4];
            final byte b2 = bytes[index - 5];
            final byte b1 = bytes[index - 6];
            final byte b0 = bytes[index - 7];
            h = PRIME_8 * h +
                PRIME_7 * b7 +
                PRIME_6 * b6 +
                PRIME_5 * b5 +
                PRIME_4 * b4 +
                PRIME_3 * b3 +
                PRIME_2 * b2 +
                PRIME_1 * b1 +
                b0;
            index -= Long.BYTES;
        }
        return h;
    }

    private static int unrolledHashCode(byte[] bytes, int index, int bytesCount, int h) {
        // there is still the hash data dependency but is more friendly
        // then a plain loop, given that we know no loop is needed here
        assert bytesCount > 0 && bytesCount < 8;
        h = PRIME_1 * h + bytes[index];
        if (bytesCount == 1) {
            return h;
        }
        h = PRIME_1 * h + bytes[index - 1];
        if (bytesCount == 2) {
            return h;
        }
        h = PRIME_1 * h + bytes[index - 2];
        if (bytesCount == 3) {
            return h;
        }
        h = PRIME_1 * h + bytes[index - 3];
        if (bytesCount == 4) {
            return h;
        }
        h = PRIME_1 * h + bytes[index - 4];
        if (bytesCount == 5) {
            return h;
        }
        h = PRIME_1 * h + bytes[index - 5];
        if (bytesCount == 6) {
            return h;
        }
        h = PRIME_1 * h + bytes[index - 6];
        return h;
    }
}
