/*
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
 */
package org.apache.qpid.proton.engine.impl;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.security.SaslFrameBody;
import org.apache.qpid.proton.amqp.security.SaslInit;
import org.apache.qpid.proton.amqp.transport.FrameBody;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.ByteBufferDecoder;
import org.apache.qpid.proton.codec.DecodeException;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.engine.Transport;
import org.apache.qpid.proton.engine.TransportException;
import org.junit.Test;

/**
 * TODO test case where input provides frame and half etc
 */
public class SaslFrameParserTest
{
    private final SaslFrameHandler _mockSaslFrameHandler = mock(SaslFrameHandler.class);
    private final ByteBufferDecoder _mockDecoder = mock(ByteBufferDecoder.class);
    private final TransportImpl mockTransport = mock(TransportImpl.class);
    private final SaslFrameParser _frameParser;
    private final SaslFrameParser _frameParserWithMockDecoder = new SaslFrameParser(_mockSaslFrameHandler, _mockDecoder, Transport.MIN_MAX_FRAME_SIZE, mockTransport);
    private final AmqpFramer _amqpFramer = new AmqpFramer();

    private final SaslInit _saslFrameBody;
    private final ByteBuffer _saslFrameBytes;

    public SaslFrameParserTest()
    {
        DecoderImpl decoder = new DecoderImpl();
        EncoderImpl encoder = new EncoderImpl(decoder);
        AMQPDefinedTypes.registerAllTypes(decoder,encoder);

        _frameParser = new SaslFrameParser(_mockSaslFrameHandler, decoder, Transport.MIN_MAX_FRAME_SIZE, mockTransport);
        _saslFrameBody = new SaslInit();
        _saslFrameBody.setMechanism(Symbol.getSymbol("unused"));
        _saslFrameBytes = ByteBuffer.wrap(_amqpFramer.generateSaslFrame(0, new byte[0], _saslFrameBody));
    }

    @Test
    public void testInputOfValidFrame()
    {
        sendAmqpSaslHeader(_frameParser);

        when(_mockSaslFrameHandler.isDone()).thenReturn(false);

        _frameParser.input(_saslFrameBytes);

        verify(_mockSaslFrameHandler).handle(isA(SaslInit.class), (Binary)isNull());
    }

    /*
     * Test that SASL frames indicating they are over 512 bytes (the minimum max frame size, and the
     * largest usable during SASL before max frame size is later determined by the Open frames) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidSizeExceedingMinMaxFrameSize()
    {
        sendAmqpSaslHeader(_frameParserWithMockDecoder);

        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp43536
        // Description: '513byte sized' SASL frame header
        byte[] oversizedSaslFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x02, 0x01, 0x02, 0x01, 0x00, 0x00 };

        try {
            _frameParserWithMockDecoder.input(ByteBuffer.wrap(oversizedSaslFrameHeader));
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("frame size 513 larger than maximum"));
        }
    }

    /*
     * Test that when the frame parser is created with a size limit above the 512 byte min-max-frame-size, frames
     * arriving with headers indicating they are over this size causes an exception.
     */
    @Test
    public void testInputOfFrameWithInvalidSizeWhenSpecifyingLargeMaxFrameSize()
    {
        SaslFrameParser frameParserWithLargeMaxSize = new SaslFrameParser(_mockSaslFrameHandler, _mockDecoder, 2017, mockTransport);
        sendAmqpSaslHeader(frameParserWithLargeMaxSize);

        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp43536
        // Description: '2057byte sized' SASL frame header
        byte[] oversizedSaslFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x08, 0x09, 0x02, 0x01, 0x00, 0x00 };

        try {
            frameParserWithLargeMaxSize.input(ByteBuffer.wrap(oversizedSaslFrameHeader));
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("frame size 2057 larger than maximum SASL frame size 2017"));
        }
    }

    /*
     * Test that SASL frames indicating they are under 8 bytes (the minimum size of the frame header) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidSizeBelowMinimumPossible()
    {
        sendAmqpSaslHeader(_frameParserWithMockDecoder);

        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp43536
        // Description: '7byte sized' SASL frame header
        byte[] undersizedSaslFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x07, 0x02, 0x01, 0x00, 0x00 };

        try {
            _frameParserWithMockDecoder.input(ByteBuffer.wrap(undersizedSaslFrameHeader));
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("frame size 7 smaller than minimum"));
        }
    }

    /*
     * Test that SASL frames indicating a doff under 8 bytes (the minimum size of the frame header) causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidDoffBelowMinimumPossible()
    {
        sendAmqpSaslHeader(_frameParserWithMockDecoder);

        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp43536
        // Description: '8byte sized' SASL frame header with invalid doff of 1[*4 = 4bytes]
        byte[] underMinDoffSaslFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x01, 0x01, 0x00, 0x00 };

        try {
            _frameParserWithMockDecoder.input(ByteBuffer.wrap(underMinDoffSaslFrameHeader));
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("data offset 4 smaller than minimum"));
        }
    }

    /*
     * Test that SASL frames indicating a doff larger than the frame size causes an error.
     */
    @Test
    public void testInputOfFrameWithInvalidDoffAboveMaximumPossible()
    {
        sendAmqpSaslHeader(_frameParserWithMockDecoder);

        // http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-security-v1.0-os.html#doc-idp43536
        // Description: '8byte sized' SASL frame header with invalid doff of 3[*4 = 12bytes]
        byte[] overFrameSizeDoffSaslFrameHeader = new byte[] { (byte) 0x00, 0x00, 0x00, 0x08, 0x03, 0x01, 0x00, 0x00 };

        try {
            _frameParserWithMockDecoder.input(ByteBuffer.wrap(overFrameSizeDoffSaslFrameHeader));
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("data offset 12 larger than the frame size 8"));
        }
    }

    @Test
    public void testInputOfInvalidFrame_causesErrorAndRefusesFurtherInput()
    {
        sendAmqpSaslHeader(_frameParserWithMockDecoder);

        String exceptionMessage = "dummy decode exception";
        when(_mockDecoder.readObject()).thenThrow(new DecodeException(exceptionMessage));

        // We send a valid frame but the mock decoder has been configured to reject it
        try {
            _frameParserWithMockDecoder.input(_saslFrameBytes);
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString(exceptionMessage));
        }

        verify(_mockSaslFrameHandler, never()).handle(any(SaslFrameBody.class), any(Binary.class));

        // Check that any further interaction causes an error TransportResult.
        try {
            _frameParserWithMockDecoder.input(ByteBuffer.wrap("".getBytes()));
            fail("expected exception");
        } catch (TransportException e) {
            // this is expected
        }
    }

    @Test
    public void testInputOfNonSaslFrame_causesErrorAndRefusesFurtherInput()
    {
        sendAmqpSaslHeader(_frameParserWithMockDecoder);

        FrameBody nonSaslFrame = new Open();
        when(_mockDecoder.readObject()).thenReturn(nonSaslFrame);

        // We send a valid frame but the mock decoder has been configured to reject it
        try {
            _frameParserWithMockDecoder.input(_saslFrameBytes);
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("Unexpected frame type encountered."));
        }

        verify(_mockSaslFrameHandler, never()).handle(any(SaslFrameBody.class), any(Binary.class));

        // Check that any further interaction causes an error TransportResult.
        try {
            _frameParserWithMockDecoder.input(ByteBuffer.wrap("".getBytes()));
            fail("expected exception");
        } catch (TransportException e) {
            // this is expected
        }
    }

    /*
     * Test that if the first 8 bytes don't match the AMQP SASL header, it causes an error.
     */
    @Test
    public void testInputOfInvalidHeader() {
        for (int invalidIndex = 0; invalidIndex < 8; invalidIndex++) {
            doInputOfInvalidHeaderTestImpl(invalidIndex);
        }
    }

    private void doInputOfInvalidHeaderTestImpl(int invalidIndex) {
        SaslFrameHandler mockSaslFrameHandler = mock(SaslFrameHandler.class);
        ByteBufferDecoder mockDecoder = mock(ByteBufferDecoder.class);
        TransportImpl mockTransport = mock(TransportImpl.class);

        SaslFrameParser saslFrameParser = new SaslFrameParser(mockSaslFrameHandler, mockDecoder, Transport.MIN_MAX_FRAME_SIZE, mockTransport);

        byte[] header = Arrays.copyOf(AmqpHeader.SASL_HEADER, AmqpHeader.SASL_HEADER.length);
        header[invalidIndex] = 'X';

        try {
            saslFrameParser.input(ByteBuffer.wrap(header));
            fail("expected exception");
        } catch (TransportException e) {
            assertThat(e.getMessage(), containsString("AMQP SASL header mismatch"));
            assertThat(e.getMessage(), containsString("In state: HEADER" + invalidIndex));
        }

        // Check that further interaction throws TransportException.
        try {
            saslFrameParser.input(ByteBuffer.wrap(new byte[0]));
            fail("expected exception");
        } catch (TransportException e) {
            // Expected
        }
    }

    /*
     * Test that if the first 8 bytes, fed in one at a time, don't match the AMQP SASL header, it causes an error.
     */
    @Test
    public void testInputOfValidHeaderInSegments()
    {
        sendAmqpSaslHeaderInSegments();

        // Try feeding in an actual frame now to check we get past the header parsing ok
        when(_mockSaslFrameHandler.isDone()).thenReturn(false);

        _frameParser.input(_saslFrameBytes);

        verify(_mockSaslFrameHandler).handle(isA(SaslInit.class), (Binary)isNull());
    }

    private void sendAmqpSaslHeaderInSegments()
    {
        for (int headerIndex = 0; headerIndex < 8; headerIndex++)
        {
            byte headerPart = AmqpHeader.SASL_HEADER[headerIndex];
            _frameParser.input(ByteBuffer.wrap(new byte[] { headerPart }));
        }
    }

    private void sendAmqpSaslHeader(SaslFrameParser saslFrameParser)
    {
        saslFrameParser.input(ByteBuffer.wrap(AmqpHeader.SASL_HEADER));
    }

}
