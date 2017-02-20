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
package org.apache.qpid.proton.engine;

import org.apache.qpid.proton.amqp.transport.DeliveryState;

/**
 * A delivery of a message on a particular link.
 *
 * Whilst a message is logically a long-lived object, a delivery is short-lived - it
 * is only intended to be used by the application until it is settled and all its data has been read.
 */
public interface Delivery extends Extendable
{

    public byte[] getTag();

    public Link getLink();

    public DeliveryState getLocalState();

    public DeliveryState getRemoteState();

    /**
     * updates the state of the delivery
     *
     * @param state the new delivery state
     */
    public void disposition(DeliveryState state);

    /**
     * Settles this delivery.
     *
     * Causes the delivery to be removed from the connection's work list (see {@link Connection#getWorkHead()}).
     * If this delivery is its link's current delivery, the link's current delivery pointer is advanced.
     */
    public void settle();

    /** This will expose the length of the current delivery buffer. */
    int getDataLength();

    /**
     * Returns whether this delivery has been settled.
     *
     * TODO proton-j and proton-c return the local and remote statuses respectively. Resolve this ambiguity.
     *
     * @see #settle()
     */
    public boolean isSettled();

    public boolean remotelySettled();

    /**
     * TODO When does an application call this method?  Do we really need this?
     */
    public void free();

    /**
     * @see Connection#getWorkHead()
     */
    public Delivery getWorkNext();

    public Delivery next();

    public boolean isWritable();

    /**
     * Returns whether this delivery has data ready to be received.
     *
     * @see Receiver#recv(byte[], int, int)
     */
    public boolean isReadable();

    public void setContext(Object o);

    public Object getContext();

    /**
     * Returns whether this delivery's state or settled flag has ever remotely changed.
     *
     * TODO what is the main intended use case for calling this method?
     */
    public boolean isUpdated();

    public void clear();

    public boolean isPartial();

    public int pending();

    public boolean isBuffered();

    /**
     * Configures a default DeliveryState to be used if a
     * received delivery is settled/freed without any disposition
     * state having been previously applied.
     *
     * @param state the default delivery state
     */
    public void setDefaultDeliveryState(DeliveryState state);

    public DeliveryState getDefaultDeliveryState();

    /**
     * Sets the message-format for this Delivery, representing the 32bit value using an int.
     *
     * The default value is 0 as per the message format defined in the core AMQP 1.0 specification.<p>
     *
     * See the following for more details:<br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-transfer</a><br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-message-format">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html#type-message-format</a><br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#section-message-format</a><br>
     * <a href="http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#definition-MESSAGE-FORMAT">
     *          http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#definition-MESSAGE-FORMAT</a><br>
     *
     * @param messageFormat the message format
     */
    public void setMessageFormat(int messageFormat);

    /**
     * Gets the message-format for this Delivery, representing the 32bit value using an int.
     *
     * @return the message-format
     * @see #setMessageFormat(int)
     */
    public int getMessageFormat();

}
