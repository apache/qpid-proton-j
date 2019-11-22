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

package org.apache.qpid.proton.engine.impl;

import static org.apache.qpid.proton.engine.impl.AmqpHeader.SASL_HEADER;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.security.SaslFrameBody;
import org.apache.qpid.proton.codec.ByteBufferDecoder;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.engine.TransportDecodeException;
import org.apache.qpid.proton.engine.TransportException;

class SaslFrameParser
{
    private static final String HEADER_DESCRIPTION = "SASL";

    private SaslFrameHandler _sasl;

    enum State
    {
        HEADER0,
        HEADER1,
        HEADER2,
        HEADER3,
        HEADER4,
        HEADER5,
        HEADER6,
        HEADER7,
        SIZE_0,
        SIZE_1,
        SIZE_2,
        SIZE_3,
        PRE_PARSE,
        BUFFERING,
        PARSING,
        ERROR
    }

    private State _state = State.HEADER0;
    private int _size;

    private ByteBuffer _buffer;

    private final ByteBufferDecoder _decoder;
    private int _frameSizeLimit;
    private TransportImpl _transport;

    SaslFrameParser(SaslFrameHandler sasl, ByteBufferDecoder decoder, int frameSizeLimit, TransportImpl transport)
    {
        _sasl = sasl;
        _decoder = decoder;
        _frameSizeLimit = frameSizeLimit;
        _transport = transport;
    }

    /**
     * Parse the provided SASL input and call my SASL frame handler with the result
     */
    public void input(ByteBuffer input) throws TransportException
    {
        TransportException frameParsingError = null;
        int size = _size;
        State state = _state;
        ByteBuffer oldIn = null;

        while(input.hasRemaining() && state != State.ERROR && !_sasl.isDone())
        {
            switch(state)
            {
                case HEADER0:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[0])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[0], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER1;
                    }
                    else
                    {
                        break;
                    }
                case HEADER1:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[1])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[1], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER2;
                    }
                    else
                    {
                        break;
                    }
                case HEADER2:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[2])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[2], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER3;
                    }
                    else
                    {
                        break;
                    }
                case HEADER3:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[3])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[3], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER4;
                    }
                    else
                    {
                        break;
                    }
                case HEADER4:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[4])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[4], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER5;
                    }
                    else
                    {
                        break;
                    }
                case HEADER5:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[5])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[5], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER6;
                    }
                    else
                    {
                        break;
                    }
                case HEADER6:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[6])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[6], state);
                            state = State.ERROR;
                            break;
                        }
                        state = State.HEADER7;
                    }
                    else
                    {
                        break;
                    }
                case HEADER7:
                    if(input.hasRemaining())
                    {
                        byte c = input.get();
                        if(c != SASL_HEADER[7])
                        {
                            frameParsingError = new TransportException("AMQP SASL header mismatch value %x, expecting %x. In state: %s", c, SASL_HEADER[7], state);
                            state = State.ERROR;
                            break;
                        }

                        logHeader();

                        state = State.SIZE_0;
                    }
                    else
                    {
                        break;
                    }
                case SIZE_0:
                    if(!input.hasRemaining())
                    {
                        break;
                    }

                    if(input.remaining() >= 4)
                    {
                        size = input.getInt();
                        state = State.PRE_PARSE;
                        break;
                    }
                    else
                    {
                        size = (input.get() << 24) & 0xFF000000;
                        if(!input.hasRemaining())
                        {
                            state = State.SIZE_1;
                            break;
                        }
                    }
                case SIZE_1:
                    size |= (input.get() << 16) & 0xFF0000;
                    if(!input.hasRemaining())
                    {
                        state = State.SIZE_2;
                        break;
                    }
                case SIZE_2:
                    size |= (input.get() << 8) & 0xFF00;
                    if(!input.hasRemaining())
                    {
                        state = State.SIZE_3;
                        break;
                    }
                case SIZE_3:
                    size |= input.get() & 0xFF;
                    state = State.PRE_PARSE;

                case PRE_PARSE:
                    if(size < 8)
                    {
                        frameParsingError = new TransportException(
                                "specified frame size %d smaller than minimum SASL frame header size 8", size);
                        state = State.ERROR;
                        break;
                    }

                    if (size > _frameSizeLimit)
                    {
                        frameParsingError = new TransportException(
                                "specified frame size %d larger than maximum SASL frame size %d", size, _frameSizeLimit);
                        state = State.ERROR;
                        break;
                    }

                    if(input.remaining() < size-4)
                    {
                        _buffer = ByteBuffer.allocate(size-4);
                        _buffer.put(input);
                        state = State.BUFFERING;
                        break;
                    }
                case BUFFERING:
                    if(_buffer != null)
                    {
                        if(input.remaining() < _buffer.remaining())
                        {
                            _buffer.put(input);
                            break;
                        }
                        else
                        {
                            ByteBuffer dup = input.duplicate();
                            dup.limit(dup.position()+_buffer.remaining());
                            input.position(input.position()+_buffer.remaining());
                            _buffer.put(dup);
                            oldIn = input;
                            _buffer.flip();
                            input = _buffer;
                            state = State.PARSING;
                        }
                    }

                case PARSING:

                    int dataOffset = (input.get() << 2) & 0x3FF;

                    if(dataOffset < 8)
                    {
                        frameParsingError = new TransportException("specified frame data offset %d smaller than minimum frame header size %d", dataOffset, 8);
                        state = State.ERROR;
                        break;
                    }
                    else if(dataOffset > size)
                    {
                        frameParsingError = new TransportException("specified frame data offset %d larger than the frame size %d", dataOffset, size);
                        state = State.ERROR;
                        break;
                    }

                    // type

                    int type = input.get() & 0xFF;
                    // SASL frame has no type-specific content in the frame header, so we skip next two bytes
                    input.get();
                    input.get();

                    if(type != SaslImpl.SASL_FRAME_TYPE)
                    {
                        frameParsingError = new TransportException("unknown frame type: %d", type);
                        state = State.ERROR;
                        break;
                    }

                    if(dataOffset!=8)
                    {
                        input.position(input.position()+dataOffset-8);
                    }

                    // oldIn null iff not working on duplicated buffer
                    if(oldIn == null)
                    {
                        oldIn = input;
                        input = input.duplicate();
                        final int endPos = input.position() + size - dataOffset;
                        input.limit(endPos);
                        oldIn.position(endPos);

                    }

                    try
                    {
                        _decoder.setByteBuffer(input);
                        Object val = _decoder.readObject();

                        Binary payload;

                        if(input.hasRemaining())
                        {
                            byte[] payloadBytes = new byte[input.remaining()];
                            input.get(payloadBytes);
                            payload = new Binary(payloadBytes);
                        }
                        else
                        {
                            payload = null;
                        }

                        if(val instanceof SaslFrameBody)
                        {
                            SaslFrameBody frameBody = (SaslFrameBody) val;
                            _sasl.handle(frameBody, payload);

                            reset();
                            input = oldIn;
                            oldIn = null;
                            _buffer = null;
                            state = State.SIZE_0;
                        }
                        else
                        {
                            state = State.ERROR;
                            frameParsingError = new TransportException("Unexpected frame type encountered."
                                                                       + " Found a %s which does not implement %s",
                                                                       val == null ? "null" : val.getClass(), SaslFrameBody.class);
                        }
                    }
                    catch (DecodeException ex)
                    {
                        state = State.ERROR;
                        String message = ex.getMessage() == null ? ex.toString(): ex.getMessage();

                        frameParsingError = new TransportDecodeException(message, ex);
                    }
                    break;
                case ERROR:
                    // do nothing
            }

        }

        _state = state;
        _size = size;

        if(_state == State.ERROR)
        {
            if(frameParsingError != null)
            {
                throw frameParsingError;
            }
            else
            {
                throw new TransportException("Unable to parse, probably because of a previous error");
            }
        }
    }

    private void reset()
    {
        _size = 0;
        _state = State.SIZE_0;
    }

    private void logHeader()
    {
        if (_transport.isFrameTracingEnabled())
        {
            _transport.log(TransportImpl.INCOMING, HEADER_DESCRIPTION);

            ProtocolTracer tracer = _transport.getProtocolTracer();
            if (tracer != null)
            {
                tracer.receivedHeader(HEADER_DESCRIPTION);
            }
        }
    }
}
