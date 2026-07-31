package org.apache.qpid.proton.codec;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.codec.ReadableBuffer.ByteBufferReader;

abstract class AbstractSymbolDecoder implements DecoderImpl.TypeDecoder<Symbol>
{
    private final int _maxCacheSize;
    private final int _maxCachedSymbolSize;
    private final ConcurrentMap<ReadableBuffer, Symbol> _symbolCache;

    public AbstractSymbolDecoder(final int maxCacheSize, final int maxCachedSymbolSize, final ConcurrentMap<ReadableBuffer, Symbol> symbolCache)
    {
        _maxCacheSize = maxCacheSize;
        _maxCachedSymbolSize = maxCachedSymbolSize;
        _symbolCache = symbolCache;
    }

    @Override
    public Symbol decode(DecoderImpl decoder, ReadableBuffer buffer)
    {
        final int remaining = buffer.remaining();
        final boolean canCache = remaining <= _maxCachedSymbolSize;

        Symbol symbol = canCache ? _symbolCache.get(buffer) : null;
        if (symbol != null)
        {
            buffer.position(buffer.limit());
            return symbol;
        }

        final byte[] bytes = new byte[remaining];
        buffer.get(bytes);

        final String symString = new String(bytes, StandardCharsets.US_ASCII);
        symbol = createSymbol(symString);

        if (canCache)
        {
            final ByteBufferReader keyBuffer = ReadableBuffer.ByteBufferReader.wrap(bytes);
            synchronized (_symbolCache)
            {
                final Symbol existing = _symbolCache.get(keyBuffer);
                if (existing != null)
                {
                    return existing;
                }

                if (_symbolCache.size() < _maxCacheSize)
                {
                    _symbolCache.put(keyBuffer, symbol);
                }
            }
        }

        return symbol;
    }

    public abstract Symbol createSymbol(String string);
}