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
package org.apache.qpid.proton.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Test the {@link Symbol} values.
 */
public class SymbolTest {

    private final String LARGE_SYMBOL_VALUE = "Large String: " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog. " +
        "The quick brown fox jumps over the lazy dog.";

    private final String MEDIUM_SYMBOL_VALUE = "Medium String: " +
         "The quick brown fox jumps over the lazy dog.";

    @Test
    public void testGetSymbolWithNullString() {
        assertNull(Symbol.getSymbol((String) null));
    }

    @Test
    public void testGetSaslSymbolWithNullString() {
        assertNull(Symbol.getSaslSymbol((String) null));
    }

    @Test
    public void testGetSymbolWithEmptyString() {
        assertNotNull(Symbol.getSymbol(""));
        assertSame(Symbol.getSymbol(""), Symbol.getSymbol(""));
    }

    @Test
    public void testGetSaslSymbolWithEmptyString() {
        assertNotNull(Symbol.getSaslSymbol(""));
        assertSame(Symbol.getSaslSymbol(""), Symbol.getSaslSymbol(""));
    }

    @Test
    public void testCompareTo() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";
        String symbolString3 = "Symbol-3";

        Symbol symbol1 = Symbol.getSymbol(symbolString1);
        Symbol symbol2 = Symbol.getSymbol(symbolString2);
        Symbol symbol3 = Symbol.getSymbol(symbolString3);

        assertEquals(0, symbol1.compareTo(symbol1));
        assertEquals(0, symbol2.compareTo(symbol2));
        assertEquals(0, symbol3.compareTo(symbol3));

        assertTrue(symbol2.compareTo(symbol1) > 0);
        assertTrue(symbol3.compareTo(symbol1) > 0);
        assertTrue(symbol3.compareTo(symbol2) > 0);

        assertTrue(symbol1.compareTo(symbol2) < 0);
        assertTrue(symbol1.compareTo(symbol3) < 0);
        assertTrue(symbol2.compareTo(symbol3) < 0);
    }

    @Test
    public void testEquals() {
        final String symbolString1 = "Symbol-1";
        final String symbolString2 = "Symbol-2";
        final String symbolString3 = "Symbol-3";

        final Symbol symbol1 = Symbol.getSymbol(symbolString1);
        final Symbol symbol2 = Symbol.getSymbol(symbolString2);
        final Symbol symbol3 = Symbol.getSymbol(symbolString3);

        assertEquals(symbolString1, symbol1.toString());
        assertEquals(symbolString2, symbol2.toString());
        assertEquals(symbolString3, symbol3.toString());

        assertNotEquals(symbol1, symbol2);
        assertNotEquals(symbol2, symbol3);
        assertNotEquals(symbol3, symbol1);

        assertNotEquals(symbol1, symbolString1);
        assertNotEquals(symbol2, symbolString2);
        assertNotEquals(symbol3, symbolString3);

        assertNotEquals(symbol1, null);
        assertNotEquals(symbol2, null);
        assertNotEquals(symbol3, null);

        final Symbol symbol1B = Symbol.getSymbol(symbolString1);

        assertEquals(symbol1, symbol1B);
        assertEquals(symbol1, symbol1); // Itself

        final Symbol largeSymbolA = Symbol.getSymbol(LARGE_SYMBOL_VALUE);
        final Symbol largeSymbolB = Symbol.getSymbol(LARGE_SYMBOL_VALUE);
        final Symbol largeSymbolC = Symbol.getSymbol(LARGE_SYMBOL_VALUE + "C");

        assertNotEquals(symbol1, largeSymbolA);
        assertNotEquals(largeSymbolA, symbol1);
        assertNotEquals(largeSymbolA, largeSymbolC);
        assertNotEquals(largeSymbolC, largeSymbolA);

        assertEquals(largeSymbolA, largeSymbolB);
        assertEquals(largeSymbolB, largeSymbolA);

        assertEquals(largeSymbolA, largeSymbolA); // Itself

        assertNotEquals(largeSymbolA, null);

        final Symbol saslSymbol1 = Symbol.getSymbol(symbolString1);
        final Symbol saslSymbol2 = Symbol.getSymbol(symbolString2);

        assertEquals(symbol1, saslSymbol1);
        assertEquals(saslSymbol1, symbol1);

        assertNotEquals(symbol2, saslSymbol1);
        assertNotEquals(saslSymbol1, symbol2);

        assertNotEquals(saslSymbol1, saslSymbol2);
        assertNotEquals(saslSymbol2, saslSymbol1);
    }

    @Test
    public void testHashcode() {
        final String symbolString1 = "Symbol-1";
        final String symbolString2 = "Symbol-2";

        final Symbol symbol1 = Symbol.getSymbol(symbolString1);
        final Symbol symbol2 = Symbol.getSymbol(symbolString2);

        assertNotEquals(symbol1, symbol2);
        assertNotEquals(symbol1.hashCode(), symbol2.hashCode());

        assertEquals(symbol1.hashCode(), Symbol.getSymbol(symbolString1).hashCode());
        assertEquals(symbol2.hashCode(), Symbol.getSymbol(symbolString2).hashCode());
        assertEquals(symbol1.hashCode(), Symbol.getSaslSymbol(symbolString1).hashCode());
        assertEquals(symbol2.hashCode(), Symbol.getSaslSymbol(symbolString2).hashCode());

        final Symbol largeSymbolA = Symbol.getSymbol(LARGE_SYMBOL_VALUE);
        final Symbol largeSymbolB = Symbol.getSymbol(LARGE_SYMBOL_VALUE);

        assertEquals(largeSymbolA, largeSymbolB);
        assertNotSame(largeSymbolA, largeSymbolB);
        assertEquals(largeSymbolA.hashCode(), largeSymbolB.hashCode());
    }

    @Test
    public void testValueOf() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";

        Symbol symbol1 = Symbol.valueOf(symbolString1);
        Symbol symbol2 = Symbol.getSymbol(symbolString2);

        assertNotEquals(symbol1, symbol2);

        assertEquals(symbolString1, symbol1.toString());
        assertEquals(symbolString2, symbol2.toString());
    }

    @Test
    public void testValueOfProducesCachedSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.valueOf(symbolString);
        Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
    }

    @Test
    public void testGetSymbol() {
        String symbolString1 = "Symbol-1";
        String symbolString2 = "Symbol-2";

        Symbol symbol1 = Symbol.getSymbol(symbolString1);
        Symbol symbol2 = Symbol.getSymbol(symbolString2);

        assertNotEquals(symbol1, symbol2);

        assertEquals(symbolString1, symbol1.toString());
        assertEquals(symbolString2, symbol2.toString());
    }

    @Test
    public void testGetSymbolProducesCachedSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.getSymbol(symbolString);
        Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
    }

    @Test
    public void testGetSymbolAndValueOfProduceCachedSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.valueOf(symbolString);
        Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
    }

    @Test
    public void testToStringProducesCachedSingleton() {
        String symbolString = "Symbol-String";

        Symbol symbol1 = Symbol.getSymbol(symbolString);
        Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);
        assertSame(symbol1.toString(), symbol2.toString());
    }

    @Test
    public void testLargeSymbolNotCached() {
        Symbol symbol1 = Symbol.getSymbol(LARGE_SYMBOL_VALUE);
        Symbol symbol2 = Symbol.getSymbol(LARGE_SYMBOL_VALUE);

        assertNotSame(symbol1, symbol2);
    }

    @Test
    public void testLargeSaslSymbolNotCached() {
        Symbol symbol1 = Symbol.getSaslSymbol(LARGE_SYMBOL_VALUE);
        Symbol symbol2 = Symbol.getSaslSymbol(LARGE_SYMBOL_VALUE);

        assertNotSame(symbol1, symbol2);
    }

    @Test
    @Ignore("Cache is static so running has side effects.")
    public void testGetSymbolCachingMaxesOut() {
        boolean notSame = false;
        int attempts = 0;
        final String prefix = "getSymbol";

        for (attempts = 0; attempts <= Short.MAX_VALUE; attempts++) {
            final String name = prefix + String.valueOf(attempts);

            final Symbol symbol1 = Symbol.getSymbol(name);
            final Symbol symbol2 = Symbol.getSymbol(name);

            if (symbol1 != symbol2) {
                notSame = true;
                break;
            }
        }

        assertTrue(notSame);
        assertTrue(attempts > 500);
        assertTrue(attempts < 8193);
    }

    @Test
    @Ignore("Cache is static so running has side effects.")
    public void testGetSaslSymbolCachingMaxesOut() {
        boolean notSame = false;
        int attempts = 0;
        final String prefix = "getSaslSymbol";

        for (attempts = 0; attempts <= Short.MAX_VALUE; attempts++) {
            final String name = prefix + String.valueOf(attempts);

            final Symbol symbol1 = Symbol.getSaslSymbol(name);
            final Symbol symbol2 = Symbol.getSaslSymbol(name);

            if (symbol1 != symbol2) {
                notSame = true;
                break;
            }
        }

        assertTrue(notSame);
        assertTrue(attempts > 5);
        assertTrue(attempts < 256);
    }

    @Test
    public void testGetSymbolAndGetSaslSymbolUseDifferentCachesBothCache() {
        final String symbolString = "Test-Sasl-Symbol-String";

        // Try SASL first, so it caches
        final Symbol saslSymbol1 = Symbol.getSaslSymbol(symbolString);
        final Symbol saslSymbol2 = Symbol.getSaslSymbol(symbolString);

        assertEquals(symbolString, saslSymbol1.toString());
        assertEquals(symbolString, saslSymbol2.toString());

        assertSame(saslSymbol1, saslSymbol2);

        // Then try main, which never checks SASL cache.
        final Symbol symbol3 = Symbol.getSymbol(symbolString);
        final Symbol symbol4 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol3.toString());
        assertEquals(symbolString, symbol4.toString());

        assertSame(symbol3, symbol4);

        assertNotSame(saslSymbol1, symbol3);
        assertNotSame(saslSymbol2, symbol4);
    }

    @Test
    public void testGetSymbolAndGetSaslSymbolUseDifferentCachesButSaslCanReuse() {
        final String symbolString = "Test-Symbol-String";

        // Try main first, so it caches
        final Symbol symbol1 = Symbol.getSymbol(symbolString);
        final Symbol symbol2 = Symbol.getSymbol(symbolString);

        assertEquals(symbolString, symbol1.toString());
        assertEquals(symbolString, symbol2.toString());

        assertSame(symbol1, symbol2);

        // Then try SASL, which will first check the main cache to avoid dup.
        final Symbol saslSymbol3 = Symbol.getSaslSymbol(symbolString);
        final Symbol saslSymbol4 = Symbol.getSaslSymbol(symbolString);

        assertEquals(symbolString, saslSymbol3.toString());
        assertEquals(symbolString, saslSymbol4.toString());

        assertSame(saslSymbol3, saslSymbol4);

        assertSame(symbol1, saslSymbol3);
        assertSame(symbol2, saslSymbol4);
    }

    @Test
    public void testGetSymbolAndGetSaslSymbolUseDifferentCachesOnlyMainCached() {
        final Symbol saslSymbol1 = Symbol.getSaslSymbol(MEDIUM_SYMBOL_VALUE);
        final Symbol saslSymbol2 = Symbol.getSaslSymbol(MEDIUM_SYMBOL_VALUE);

        assertEquals(MEDIUM_SYMBOL_VALUE, saslSymbol1.toString());
        assertEquals(MEDIUM_SYMBOL_VALUE, saslSymbol2.toString());

        assertNotSame(saslSymbol1, saslSymbol2); // Exceeded length for SASL cache

        final Symbol symbol3 = Symbol.getSymbol(MEDIUM_SYMBOL_VALUE);
        final Symbol symbol4 = Symbol.getSymbol(MEDIUM_SYMBOL_VALUE);

        assertEquals(MEDIUM_SYMBOL_VALUE, symbol3.toString());
        assertEquals(MEDIUM_SYMBOL_VALUE, symbol4.toString());

        assertSame(symbol3, symbol4);

        assertNotSame(saslSymbol1, symbol3);
        assertNotSame(saslSymbol2, symbol4);
    }
}
