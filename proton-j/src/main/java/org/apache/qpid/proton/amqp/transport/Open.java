
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


package org.apache.qpid.proton.amqp.transport;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedShort;

@SuppressWarnings("rawtypes")
public final class Open implements FrameBody
{
    private String _containerId;
    private String _hostname;
    private UnsignedInteger _maxFrameSize = UnsignedInteger.valueOf(0xffffffff);
    private UnsignedShort _channelMax = UnsignedShort.valueOf((short) 65535);
    private UnsignedInteger _idleTimeOut;
    private Symbol[] _outgoingLocales;
    private Symbol[] _incomingLocales;
    private Symbol[] _offeredCapabilities;
    private Symbol[] _desiredCapabilities;
    private Map _properties;

    public Open() {}

    @SuppressWarnings("unchecked")
    public Open(Open other)
    {
        this._containerId = other._containerId;
        this._hostname = other._hostname;
        this._maxFrameSize = other._maxFrameSize;
        this._channelMax = other._channelMax;
        this._idleTimeOut = other._idleTimeOut;
        if (other._outgoingLocales != null) {
            this._outgoingLocales = Arrays.copyOf(other._outgoingLocales, other._outgoingLocales.length);
        }
        if (other._incomingLocales != null) {
            this._incomingLocales = Arrays.copyOf(other._incomingLocales, other._incomingLocales.length);
        }
        if (other._offeredCapabilities != null) {
            this._offeredCapabilities = Arrays.copyOf(other._offeredCapabilities, other._offeredCapabilities.length);
        }
        if (other._desiredCapabilities != null) {
            this._desiredCapabilities = Arrays.copyOf(other._desiredCapabilities, other._desiredCapabilities.length);
        }
        if (other._properties != null) {
            this._properties = new LinkedHashMap<>(other._properties);
        }
    }

    public String getContainerId()
    {
        return _containerId;
    }

    public void setContainerId(String containerId)
    {
        if( containerId == null )
        {
            throw new NullPointerException("the container-id field is mandatory");
        }

        _containerId = containerId;
    }

    public String getHostname()
    {
        return _hostname;
    }

    public void setHostname(String hostname)
    {
        _hostname = hostname;
    }

    public UnsignedInteger getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    public void setMaxFrameSize(UnsignedInteger maxFrameSize)
    {
        _maxFrameSize = maxFrameSize;
    }

    public UnsignedShort getChannelMax()
    {
        return _channelMax;
    }

    public void setChannelMax(UnsignedShort channelMax)
    {
        _channelMax = channelMax;
    }

    public UnsignedInteger getIdleTimeOut()
    {
        return _idleTimeOut;
    }

    public void setIdleTimeOut(UnsignedInteger idleTimeOut)
    {
        _idleTimeOut = idleTimeOut;
    }

    public Symbol[] getOutgoingLocales()
    {
        return _outgoingLocales;
    }

    public void setOutgoingLocales(Symbol... outgoingLocales)
    {
        _outgoingLocales = outgoingLocales;
    }

    public Symbol[] getIncomingLocales()
    {
        return _incomingLocales;
    }

    public void setIncomingLocales(Symbol... incomingLocales)
    {
        _incomingLocales = incomingLocales;
    }

    public Symbol[] getOfferedCapabilities()
    {
        return _offeredCapabilities;
    }

    public void setOfferedCapabilities(Symbol... offeredCapabilities)
    {
        _offeredCapabilities = offeredCapabilities;
    }

    public Symbol[] getDesiredCapabilities()
    {
        return _desiredCapabilities;
    }

    public void setDesiredCapabilities(Symbol... desiredCapabilities)
    {
        _desiredCapabilities = desiredCapabilities;
    }

    public Map getProperties()
    {
        return _properties;
    }

    public void setProperties(Map properties)
    {
        _properties = properties;
    }

    @Override
    public <E> void invoke(FrameBodyHandler<E> handler, Binary payload, E context)
    {
        handler.handleOpen(this, payload, context);
    }

    @Override
    public String toString()
    {
        return "Open{" +
               " containerId='" + _containerId + '\'' +
               ", hostname='" + _hostname + '\'' +
               ", maxFrameSize=" + _maxFrameSize +
               ", channelMax=" + _channelMax +
               ", idleTimeOut=" + _idleTimeOut +
               ", outgoingLocales=" + (_outgoingLocales == null ? null : Arrays.asList(_outgoingLocales)) +
               ", incomingLocales=" + (_incomingLocales == null ? null : Arrays.asList(_incomingLocales)) +
               ", offeredCapabilities=" + (_offeredCapabilities == null ? null : Arrays.asList(_offeredCapabilities)) +
               ", desiredCapabilities=" + (_desiredCapabilities == null ? null : Arrays.asList(_desiredCapabilities)) +
               ", properties=" + _properties +
               '}';
    }

    @Override
    public Open copy()
    {
        return new Open(this);
    }
}
