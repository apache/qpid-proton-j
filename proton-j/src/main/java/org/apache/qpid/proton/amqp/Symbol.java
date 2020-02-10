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

package org.apache.qpid.proton.amqp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

public final class Symbol implements Comparable<Symbol>, CharSequence
{
    private static final int[] NOT_INITIALIZED_JUMP_TABLE = new int[] {};
    private final String _underlying;
    private final byte[] _underlyingBytes;
    // Used by indexOf
    private volatile int[] _jumpTable;

    private static final ConcurrentHashMap<String, Symbol> _symbols = new ConcurrentHashMap<String, Symbol>(2048);

    private Symbol(String underlying)
    {
        _underlying = underlying;
        _underlyingBytes = underlying.getBytes(StandardCharsets.US_ASCII);
        _jumpTable = NOT_INITIALIZED_JUMP_TABLE;
    }

    public int length()
    {
        return _underlying.length();
    }

    public int compareTo(Symbol o)
    {
        return _underlying.compareTo(o._underlying);
    }

    public char charAt(int index)
    {
        return _underlying.charAt(index);
    }

    public CharSequence subSequence(int beginIndex, int endIndex)
    {
        return _underlying.subSequence(beginIndex, endIndex);
    }

    /**
     * https://en.wikipedia.org/wiki/Knuth%E2%80%93Morris%E2%80%93Pratt_algorithm jump table
     */
    private static int[] createJumpTable(byte[] needle) {
        final int[] jumpTable = new int[needle.length + 1];
        int j = 0;
        for (int i = 1; i < needle.length; i++) {
            while (j > 0 && needle[j] != needle[i]) {
                j = jumpTable[j];
            }
            if (needle[j] == needle[i]) {
                j++;
            }
            jumpTable[i + 1] = j;
        }
        for (int i = 1; i < jumpTable.length; i++) {
            if (jumpTable[i] != 0) {
                return jumpTable;
            }
        }
        // optimization over the original algorithm: it would save from accessing any jump table
        return null;
    }

    private int[] racyGerOrCreateJumpTable() {
        int[] jumpTable = this._jumpTable;
        if (jumpTable == NOT_INITIALIZED_JUMP_TABLE) {
            jumpTable = createJumpTable(this._underlyingBytes);
            _jumpTable = jumpTable;
        }
        return jumpTable;
    }

    /**
     * Returns the index on buffer of the first encoded occurrence of this {@code symbol} within the specified range.<br>
     * If none is found, then {@code -1} is returned.
     *
     * @param buffer the buffer where to search in
     * @return the index of the first occurrence of this symbol or {@code -1} if it won't occur.
     * <p>
     * @throws IllegalArgumentException if any of the indexes of the specified range is negative
     */
    public int searchFirst(ReadableBuffer buffer, int from, int to)
    {
        Objects.requireNonNull(buffer, "buffer cannot be null");
        if (from < 0 || to < 0)
        {
            throw new IllegalArgumentException("range indexes cannot be negative!");
        }
        int j = 0;
        final int[] jumpTable = racyGerOrCreateJumpTable();
        final byte[] needle = _underlyingBytes;
        final long length = to - from;
        final ReadableBuffer haystack = buffer;
        final int needleLength = needle.length;
        for (int i = 0; i < length; i++)
        {
            final int index = from + i;
            final byte value = haystack.get(index);
            while (j > 0 && needle[j] != value)
            {
                j = jumpTable == null ? 0 : jumpTable[j];
            }
            if (needle[j] == value)
            {
                j++;
            }
            if (j == needleLength)
            {
                final int startMatch = index - needleLength + 1;
                return startMatch;
            }
        }
        return -1;
    }

    @Override
    public String toString()
    {
        return _underlying;
    }

    @Override
    public int hashCode()
    {
        return _underlying.hashCode();
    }

    public static Symbol valueOf(String symbolVal)
    {
        return getSymbol(symbolVal);
    }

    public static Symbol getSymbol(String symbolVal)
    {
        if(symbolVal == null)
        {
            return null;
        }
        Symbol symbol = _symbols.get(symbolVal);
        if(symbol == null)
        {
            symbolVal = symbolVal.intern();
            symbol = new Symbol(symbolVal);
            Symbol existing;
            if((existing = _symbols.putIfAbsent(symbolVal, symbol)) != null)
            {
                symbol = existing;
            }
        }
        return symbol;
    }

    public void writeTo(WritableBuffer buffer)
    {
        buffer.put(_underlyingBytes, 0, _underlyingBytes.length);
    }

    public void writeTo(ByteBuffer buffer)
    {
        buffer.put(_underlyingBytes, 0, _underlyingBytes.length);
    }
}
