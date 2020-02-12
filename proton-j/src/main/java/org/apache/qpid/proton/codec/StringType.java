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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.Collection;

public class StringType extends AbstractPrimitiveType<String>
{
    private static final DecoderImpl.TypeDecoder<String> _stringCreator =
        new DecoderImpl.TypeDecoder<String>()
        {
            @Override
            public String decode(DecoderImpl decoder, final ReadableBuffer buffer)
            {
                CharsetDecoder charsetDecoder = decoder.getCharsetDecoder();
                try
                {
                    return buffer.readString(charsetDecoder);
                }
                catch (CharacterCodingException e)
                {
                    throw new IllegalArgumentException("Cannot parse String", e);
                }
                finally
                {
                    charsetDecoder.reset();
                }
            }
        };

    public static interface StringEncoding extends PrimitiveTypeEncoding<String>
    {
        void setValue(String val, int length);
    }

    private final StringEncoding _stringEncoding;
    private final StringEncoding _shortStringEncoding;

    StringType(final EncoderImpl encoder, final DecoderImpl decoder)
    {
        _stringEncoding = new AllStringEncoding(encoder, decoder);
        _shortStringEncoding = new ShortStringEncoding(encoder, decoder);
        encoder.register(String.class, this);
        decoder.register(this);
    }

    @Override
    public Class<String> getTypeClass()
    {
        return String.class;
    }

    @Override
    public StringEncoding getEncoding(final String val)
    {
        final int length = calculateUTF8Length(val);
        StringEncoding encoding = length <= 255
                ? _shortStringEncoding
                : _stringEncoding;
        encoding.setValue(val, length);
        return encoding;
    }

    static int calculateUTF8Length(final String s)
    {
        final int stringLength = s.length();

        // ASCII Optimized length case
        int utf8len = stringLength;
        int processed = 0;
        for (; processed < stringLength && s.charAt(processed) < 0x80; processed++) {}

        if (processed < stringLength)
        {
            // Non-ASCII length remainder
            utf8len = extendedCalculateUTF8Length(s, processed, stringLength, utf8len);
        }

        return utf8len;
    }

    static int extendedCalculateUTF8Length(final String s, int index, int length, int utf8len) {
        for (; index < length; index++)
        {
            int c = s.charAt(index);
            if ((c & 0xFF80) != 0)         /* U+0080..    */
            {
                utf8len++;
                if(((c & 0xF800) != 0))    /* U+0800..    */
                {
                    utf8len++;
                    // surrogate pairs should always combine to create a code point with a 4 octet representation
                    if ((c & 0xD800) == 0xD800 && c < 0xDC00)
                    {
                        index++;
                    }
                }
            }
        }

        return utf8len;
    }

    @Override
    public StringEncoding getCanonicalEncoding()
    {
        return _stringEncoding;
    }

    @Override
    public Collection<StringEncoding> getAllEncodings()
    {
        return Arrays.asList(_shortStringEncoding, _stringEncoding);
    }

    private class AllStringEncoding
            extends LargeFloatingSizePrimitiveTypeEncoding<String>
            implements StringEncoding
    {
        private String _value;
        private int _length;

        public AllStringEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final String val)
        {
            getEncoder().getBuffer().ensureRemaining(getEncodedValueSize(val));
            getEncoder().writeRaw(val);
        }

        @Override
        protected int getEncodedValueSize(final String val)
        {
            return (val == _value) ? _length : calculateUTF8Length(val);
        }

        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.STR32;
        }

        @Override
        public StringType getType()
        {
            return StringType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<String> encoding)
        {
            return (getType() == encoding.getType());
        }

        @Override
        public String readValue()
        {
            DecoderImpl decoder = getDecoder();
            int size = decoder.readRawInt();
            return size == 0 ? "" : decoder.readRaw(_stringCreator, size);
        }

        @Override
        public void setValue(final String val, final int length)
        {
            _value = val;
            _length = length;
        }

        @Override
        public void skipValue()
        {
            DecoderImpl decoder = getDecoder();
            ReadableBuffer buffer = decoder.getBuffer();
            int size = decoder.readRawInt();
            buffer.position(buffer.position() + size);
        }
    }

    private class ShortStringEncoding
            extends SmallFloatingSizePrimitiveTypeEncoding<String>
            implements StringEncoding
    {
        private String _value;
        private int _length;

        public ShortStringEncoding(final EncoderImpl encoder, final DecoderImpl decoder)
        {
            super(encoder, decoder);
        }

        @Override
        protected void writeEncodedValue(final String val)
        {
            getEncoder().getBuffer().ensureRemaining(getEncodedValueSize(val));
            getEncoder().writeRaw(val);
        }

        @Override
        protected int getEncodedValueSize(final String val)
        {
            return (val == _value) ? _length : calculateUTF8Length(val);
        }

        @Override
        public byte getEncodingCode()
        {
            return EncodingCodes.STR8;
        }

        @Override
        public StringType getType()
        {
            return StringType.this;
        }

        @Override
        public boolean encodesSuperset(final TypeEncoding<String> encoder)
        {
            return encoder == this;
        }

        @Override
        public String readValue()
        {
            DecoderImpl decoder = getDecoder();
            int size = ((int)decoder.readRawByte()) & 0xff;
            return size == 0 ? "" : decoder.readRaw(_stringCreator, size);
        }

        @Override
        public void setValue(final String val, final int length)
        {
            _value = val;
            _length = length;
        }

        @Override
        public void skipValue()
        {
            DecoderImpl decoder = getDecoder();
            ReadableBuffer buffer = decoder.getBuffer();
            int size = ((int)decoder.readRawByte()) & 0xff;
            buffer.position(buffer.position() + size);
        }
    }
}
