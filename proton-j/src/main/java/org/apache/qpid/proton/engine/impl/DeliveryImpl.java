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

import java.util.Arrays;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.codec.CompositeReadableBuffer;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Record;
import org.apache.qpid.proton.engine.Transport;

public class DeliveryImpl implements Delivery
{
    public static final int DEFAULT_MESSAGE_FORMAT = 0;

    private static final ReadableBuffer EMPTY_BUFFER = ReadableBuffer.ByteBufferReader.allocate(0);

    private DeliveryImpl _linkPrevious;
    private DeliveryImpl _linkNext;

    private DeliveryImpl _workNext;
    private DeliveryImpl _workPrev;
    boolean _work;

    private DeliveryImpl _transportWorkNext;
    private DeliveryImpl _transportWorkPrev;
    boolean _transportWork;

    private Record _attachments;
    private Object _context;

    private final byte[] _tag;
    private final LinkImpl _link;
    private DeliveryState _deliveryState;
    private boolean _settled;
    private boolean _remoteSettled;
    private DeliveryState _remoteDeliveryState;
    private DeliveryState _defaultDeliveryState = null;
    private int _messageFormat = DEFAULT_MESSAGE_FORMAT;

    /**
     * A bit-mask representing the outstanding work on this delivery received from the transport layer
     * that has not yet been processed by the application.
     */
    private int _flags = (byte) 0;

    private TransportDelivery _transportDelivery;
    private boolean _complete;
    private boolean _updated;
    private boolean _done;
    private boolean _aborted;

    private CompositeReadableBuffer _dataBuffer;
    private ReadableBuffer _dataView;

    DeliveryImpl(final byte[] tag, final LinkImpl link, DeliveryImpl previous)
    {
        _tag = tag;
        _link = link;
        _link.incrementUnsettled();
        _linkPrevious = previous;
        if (previous != null)
        {
            previous._linkNext = this;
        }
    }

    @Override
    public byte[] getTag()
    {
        return _tag;
    }

    @Override
    public LinkImpl getLink()
    {
        return _link;
    }

    @Override
    public DeliveryState getLocalState()
    {
        return _deliveryState;
    }

    @Override
    public DeliveryState getRemoteState()
    {
        return _remoteDeliveryState;
    }

    @Override
    public boolean remotelySettled()
    {
        return _remoteSettled;
    }

    @Override
    public void setMessageFormat(int messageFormat)
    {
        _messageFormat = messageFormat;
    }

    @Override
    public int getMessageFormat()
    {
        return _messageFormat;
    }

    @Override
    public void disposition(final DeliveryState state)
    {
        _deliveryState = state;
        if(!_remoteSettled && !_settled)
        {
            addToTransportWorkList();
        }
    }

    @Override
    public void settle()
    {
        if (_settled) {
            return;
        }

        _settled = true;
        _link.decrementUnsettled();
        if(!_remoteSettled)
        {
            addToTransportWorkList();
        }
        else
        {
            _transportDelivery.settled();
        }

        if(_link.current() == this)
        {
            _link.advance();
        }

        _link.remove(this);
        if(_linkPrevious != null)
        {
            _linkPrevious._linkNext = _linkNext;
        }

        if(_linkNext != null)
        {
            _linkNext._linkPrevious = _linkPrevious;
        }

        updateWork();

        _linkNext= null;
        _linkPrevious = null;
    }

    DeliveryImpl getLinkNext()
    {
        return _linkNext;
    }

    @Override
    public DeliveryImpl next()
    {
        return getLinkNext();
    }

    @Override
    public void free()
    {
        settle();
    }

    DeliveryImpl getLinkPrevious()
    {
        return _linkPrevious;
    }

    @Override
    public DeliveryImpl getWorkNext()
    {
        if (_workNext != null)
            return _workNext;
        // the following hack is brought to you by the C implementation!
        if (!_work)  // not on the work list
            return _link.getConnectionImpl().getWorkHead();
        return null;
    }

    DeliveryImpl getWorkPrev()
    {
        return _workPrev;
    }

    void setWorkNext(DeliveryImpl workNext)
    {
        _workNext = workNext;
    }

    void setWorkPrev(DeliveryImpl workPrev)
    {
        _workPrev = workPrev;
    }

    int recv(final byte[] bytes, int offset, int size)
    {
        final int consumed;
        if (_dataBuffer != null && _dataBuffer.hasRemaining())
        {
            consumed = Math.min(size, _dataBuffer.remaining());

            _dataBuffer.get(bytes, offset, consumed);
            _dataBuffer.reclaimRead();
        }
        else
        {
            consumed = 0;
        }

        return (_complete && consumed == 0) ? Transport.END_OF_STREAM : consumed;  //TODO - Implement
    }

    int recv(final WritableBuffer buffer)
    {
        final int consumed;
        if (_dataBuffer != null && _dataBuffer.hasRemaining())
        {
            consumed = Math.min(buffer.remaining(), _dataBuffer.remaining());
            buffer.put(_dataBuffer);
            _dataBuffer.reclaimRead();
        }
        else
        {
            consumed = 0;
        }

        return (_complete && consumed == 0) ? Transport.END_OF_STREAM : consumed;
    }

    ReadableBuffer recv()
    {
        ReadableBuffer result = _dataView;
        if (_dataView != null)
        {
            _dataView = _dataBuffer = null;
        }
        else
        {
            result = EMPTY_BUFFER;
        }

        return result;
    }

    void updateWork()
    {
        getLink().getConnectionImpl().workUpdate(this);
    }

    DeliveryImpl clearTransportWork()
    {
        DeliveryImpl next = _transportWorkNext;
        getLink().getConnectionImpl().removeTransportWork(this);
        return next;
    }

    void addToTransportWorkList()
    {
        getLink().getConnectionImpl().addTransportWork(this);
    }

    DeliveryImpl getTransportWorkNext()
    {
        return _transportWorkNext;
    }

    DeliveryImpl getTransportWorkPrev()
    {
        return _transportWorkPrev;
    }

    void setTransportWorkNext(DeliveryImpl transportWorkNext)
    {
        _transportWorkNext = transportWorkNext;
    }

    void setTransportWorkPrev(DeliveryImpl transportWorkPrev)
    {
        _transportWorkPrev = transportWorkPrev;
    }

    TransportDelivery getTransportDelivery()
    {
        return _transportDelivery;
    }

    void setTransportDelivery(TransportDelivery transportDelivery)
    {
        _transportDelivery = transportDelivery;
    }

    @Override
    public boolean isSettled()
    {
        return _settled;
    }

    int send(byte[] bytes, int offset, int length)
    {
        byte[] copy = new byte[length];
        System.arraycopy(bytes, offset, copy, 0, length);
        getOrCreateDataBuffer().append(copy);
        addToTransportWorkList();
        return length;
    }

    int send(final ReadableBuffer buffer)
    {
        int length = buffer.remaining();
        getOrCreateDataBuffer().append(copyContents(buffer));
        addToTransportWorkList();
        return length;
    }

    int sendNoCopy(ReadableBuffer buffer)
    {
        int length = buffer.remaining();

        if (_dataView == null || !_dataView.hasRemaining())
        {
            _dataView = buffer;
        }
        else
        {
            consolidateSendBuffers(buffer);
        }

        addToTransportWorkList();
        return length;
    }

    private byte[] copyContents(ReadableBuffer buffer)
    {
        byte[] copy = new byte[buffer.remaining()];

        if (buffer.hasArray())
        {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + buffer.position(), copy, 0, buffer.remaining());
            buffer.position(buffer.limit());
        }
        else
        {
            buffer.get(copy, 0, buffer.remaining());
        }

        return copy;
    }

    private void consolidateSendBuffers(ReadableBuffer buffer)
    {
        if (_dataView == _dataBuffer)
        {
            getOrCreateDataBuffer().append(copyContents(buffer));
        }
        else
        {
            ReadableBuffer oldView = _dataView;

            CompositeReadableBuffer dataBuffer = getOrCreateDataBuffer();
            dataBuffer.append(copyContents(oldView));
            dataBuffer.append(copyContents(buffer));

            oldView.reclaimRead();
        }

        buffer.reclaimRead();  // A pooled buffer could release now.
    }

    void append(Binary payload)
    {
        byte[] data = payload.getArray();

        // The Composite buffer cannot handle composites where the array
        // is a view of a larger array so we must copy the payload into
        // an array of the exact size
        if (payload.getArrayOffset() > 0 || payload.getLength() < data.length)
        {
            data = new byte[payload.getLength()];
            System.arraycopy(payload.getArray(), payload.getArrayOffset(), data, 0, payload.getLength());
        }

        getOrCreateDataBuffer().append(data);
    }

    private CompositeReadableBuffer getOrCreateDataBuffer()
    {
        if (_dataBuffer == null)
        {
            _dataView = _dataBuffer = new CompositeReadableBuffer();
        }

        return _dataBuffer;
    }

    void append(byte[] data)
    {
        getOrCreateDataBuffer().append(data);
    }

    void afterSend()
    {
        if (_dataView != null)
        {
            _dataView.reclaimRead();
            if (!_dataView.hasRemaining())
            {
                _dataView = _dataBuffer;
            }
        }
    }

    ReadableBuffer getData()
    {
        return _dataView == null ? EMPTY_BUFFER : _dataView;
    }

    int getDataLength()
    {
        return _dataView == null ? 0 : _dataView.remaining();
    }

    @Override
    public int available()
    {
        return _dataView == null ? 0 : _dataView.remaining();
    }

    @Override
    public boolean isWritable()
    {
        return getLink() instanceof SenderImpl
                && getLink().current() == this
                && ((SenderImpl) getLink()).hasCredit();
    }

    @Override
    public boolean isReadable()
    {
        return getLink() instanceof ReceiverImpl
            && getLink().current() == this;
    }

    void setComplete()
    {
        _complete = true;
    }

    void setAborted()
    {
        _aborted = true;
    }

    @Override
    public boolean isAborted()
    {
        return _aborted;
    }

    @Override
    public boolean isPartial()
    {
        return !_complete;
    }

    void setRemoteDeliveryState(DeliveryState remoteDeliveryState)
    {
        _remoteDeliveryState = remoteDeliveryState;
        _updated = true;
    }

    @Override
    public boolean isUpdated()
    {
        return _updated;
    }

    @Override
    public void clear()
    {
        _updated = false;
        getLink().getConnectionImpl().workUpdate(this);
    }

    void setDone()
    {
        _done = true;
    }

    boolean isDone()
    {
        return _done;
    }

    void setRemoteSettled(boolean remoteSettled)
    {
        _remoteSettled = remoteSettled;
        _updated = true;
    }

    @Override
    public boolean isBuffered()
    {
        if (_remoteSettled) return false;
        if (getLink() instanceof SenderImpl) {
            if (isDone()) {
                return false;
            } else {
                boolean hasRemaining = false;
                if (_dataView != null) {
                    hasRemaining = _dataView.hasRemaining();
                }

                return _complete || hasRemaining;
            }
        } else {
            return false;
        }
    }

    @Override
    public Object getContext()
    {
        return _context;
    }

    @Override
    public void setContext(Object context)
    {
        _context = context;
    }

    @Override
    public Record attachments()
    {
        if(_attachments == null)
        {
            _attachments = new RecordImpl();
        }

        return _attachments;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("DeliveryImpl [_tag=").append(Arrays.toString(_tag))
            .append(", _link=").append(_link)
            .append(", _deliveryState=").append(_deliveryState)
            .append(", _settled=").append(_settled)
            .append(", _remoteSettled=").append(_remoteSettled)
            .append(", _remoteDeliveryState=").append(_remoteDeliveryState)
            .append(", _flags=").append(_flags)
            .append(", _defaultDeliveryState=").append(_defaultDeliveryState)
            .append(", _transportDelivery=").append(_transportDelivery)
            .append(", _data Size=").append(getDataLength())
            .append(", _complete=").append(_complete)
            .append(", _updated=").append(_updated)
            .append(", _done=").append(_done)
            .append("]");
        return builder.toString();
    }

    @Override
    public int pending()
    {
        return _dataView == null ? 0 : _dataView.remaining();
    }

    @Override
    public void setDefaultDeliveryState(DeliveryState state)
    {
        _defaultDeliveryState = state;
    }

    @Override
    public DeliveryState getDefaultDeliveryState()
    {
        return _defaultDeliveryState;
    }
}
