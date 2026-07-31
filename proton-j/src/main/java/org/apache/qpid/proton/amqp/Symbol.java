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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.proton.codec.WritableBuffer;

public final class Symbol implements Comparable<Symbol>, CharSequence
{
    private static final int MAX_CACHE_SIZE = 4096;
    private static final int MAX_CACHED_SYMBOL_LENGTH = 64;

    private static final int MAX_SASL_CACHE_SIZE = 64;
    private static final int MAX_CACHED_SASL_SYMBOL_LENGTH = 32;

    private final String _underlying;
    private final byte[] _underlyingBytes;

    private static final ConcurrentHashMap<String, Symbol> _symbols = new ConcurrentHashMap<String, Symbol>(2048);
    private static final ConcurrentHashMap<String, Symbol> _saslSymbols = new ConcurrentHashMap<String, Symbol>(32);

    private Symbol(String underlying)
    {
        _underlying = underlying;
        _underlyingBytes = underlying.getBytes(StandardCharsets.US_ASCII);
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (other instanceof Symbol) {
            return _underlying.equals(((Symbol) other)._underlying);
        }

        return false;
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
            symbol = new Symbol(symbolVal);
            if(symbol.length() <= MAX_CACHED_SYMBOL_LENGTH && _symbols.size() < MAX_CACHE_SIZE)
            {
                Symbol existing;
                if((existing = _symbols.putIfAbsent(symbolVal, symbol)) != null)
                {
                    symbol = existing;
                }
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

    public static Symbol getSaslSymbol(String symbolVal)
    {
        if(symbolVal == null)
        {
            return null;
        }

        // Lookup main cache first, then SASL-specific, to reuse where possible
        Symbol symbol = _symbols.get(symbolVal);
        if(symbol == null)
        {
            symbol = _saslSymbols.get(symbolVal);
        }

        // If no match, populate into the SASL-specific cache
        if(symbol == null)
        {
            symbol = new Symbol(symbolVal);
            if(symbol.length() <= MAX_CACHED_SASL_SYMBOL_LENGTH)
            {
                synchronized (_saslSymbols)
                {
                    final Symbol existing = _saslSymbols.get(symbolVal);
                    if (existing != null)
                    {
                        return existing;
                    }

                    if (_saslSymbols.size() < MAX_SASL_CACHE_SIZE)
                    {
                        _saslSymbols.put(symbolVal, symbol);
                    }
                }
            }
        }
        return symbol;
    }
}
