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
package org.apache.qpid.proton.message;

import java.util.Collection;
import java.util.function.Consumer;

import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.impl.MessageImpl;

/**
 * Represents a Message within Proton.
 *
 * Create instances of Message using {@link Message.Factory}.
 *
 */
public interface Message
{

    public static final class Factory
    {
        public static Message create() {
            return new MessageImpl();
        }

        public static Message create(Header header,
                                     DeliveryAnnotations deliveryAnnotations,
                                     MessageAnnotations messageAnnotations,
                                     Properties properties,
                                     ApplicationProperties applicationProperties,
                                     Section body,
                                     Footer footer) {
            return new MessageImpl(header, deliveryAnnotations,
                                   messageAnnotations, properties,
                                   applicationProperties, body, footer);
        }
    }


    short DEFAULT_PRIORITY = 4;

    boolean isDurable();

    long getDeliveryCount();

    short getPriority();

    boolean isFirstAcquirer();

    long getTtl();

    void setDurable(boolean durable);

    void setTtl(long ttl);

    void setDeliveryCount(long deliveryCount);

    void setFirstAcquirer(boolean firstAcquirer);

    void setPriority(short priority);

    Object getMessageId();

    long getGroupSequence();

    String getReplyToGroupId();


    long getCreationTime();

    String getAddress();

    byte[] getUserId();

    String getReplyTo();

    String getGroupId();

    String getContentType();

    long getExpiryTime();

    Object getCorrelationId();

    String getContentEncoding();

    String getSubject();

    void setGroupSequence(long groupSequence);

    void setUserId(byte[] userId);

    void setCreationTime(long creationTime);

    void setSubject(String subject);

    void setGroupId(String groupId);

    void setAddress(String to);

    void setExpiryTime(long absoluteExpiryTime);

    void setReplyToGroupId(String replyToGroupId);

    void setContentEncoding(String contentEncoding);

    void setContentType(String contentType);

    void setReplyTo(String replyTo);

    void setCorrelationId(Object correlationId);

    void setMessageId(Object messageId);

    Header getHeader();

    DeliveryAnnotations getDeliveryAnnotations();

    MessageAnnotations getMessageAnnotations();

    Properties getProperties();

    ApplicationProperties getApplicationProperties();

    Section getBody();

    Footer getFooter();

    void setHeader(Header header);

    void setDeliveryAnnotations(DeliveryAnnotations deliveryAnnotations);

    void setMessageAnnotations(MessageAnnotations messageAnnotations);

    void setProperties(Properties properties);

    void setApplicationProperties(ApplicationProperties applicationProperties);

    void setBody(Section body);

    void setFooter(Footer footer);

    /**
     * TODO describe what happens if the data does not represent a complete message.
     * Currently this appears to leave the message in an unknown state.
     */
    int decode(byte[] data, int offset, int length);

    /**
     * Decodes the Message from the given {@link ReadableBuffer}.
     * <p>
     * If the buffer given does not contain the fully encoded Message bytes for decode
     * this method will throw an exception to indicate the buffer underflow condition and
     * the message object will be left in an undefined state.
     *
     * @param buffer
     *      A {@link ReadableBuffer} that contains the complete message bytes.
     */
    void decode(ReadableBuffer buffer);

    /**
     * Encodes up to {@code length} bytes of the message into the provided byte array,
     * starting at position {@code offset}.
     *
     * TODO describe what happens if length is smaller than the encoded form, Currently
     * Proton-J throws an exception. What does Proton-C do?
     *
     * @return the number of bytes written to the byte array
     */
    int encode(byte[] data, int offset, int length);

    /**
     * Encodes the current Message contents into the given {@link WritableBuffer} instance.
     * <p>
     * This method attempts to encode all message data into the {@link WritableBuffer} and
     * if the buffer has insufficient space it will throw an exception to indicate the buffer
     * overflow condition.  If successful the method returns the number of bytes written to
     * the provided buffer to fully encode the message.
     *
     * @param buffer
     *      The {@link WritableBuffer} instance to encode the message contents into.
     *
     * @return the number of bytes written to fully encode the message.
     */
    int encode(WritableBuffer buffer);

    void clear();

    MessageError getError();

    /**
     * @return the total number of body {@link Section} elements contained in this {@link Message}
     */
    int getBodySectionCount();

    /**
     * Sets the body {@link Section} instances to use when encoding this message.  The value
     * given replaces any existing section(s) assigned to this message through the {@link Message#setBody(Section)}
     * or {@link #addBodySection(Section)} methods.  Calling this method with a null or empty
     * collection is equivalent to calling the {@link #clear()} method.  The collection passed is copied
     * and any future changes to it have no affect on the contents of this message's body sections collection.
     *
     * @param sections
     *      The {@link Collection} of {@link Section} instance to assign this message.
     *
     * @return this {@link Message} instance.
     */
    Message setBodySections(Collection<Section> sections);

    /**
     * Adds the given {@link Section} to the internal collection of sections that will be sent
     * to the remote peer when this message is encoded.  If a previous section was added by a call
     * to the {@link #setBody(Object)} method it should be retained as the first element of
     * the running list of body sections contained in this message.
     *
     * @param bodySection
     *      The {@link Section} instance to append to the internal collection.
     *
     * @return this {@link Message} instance.
     *
     * @throws NullPointerException if the body section value passed is null.
     */
    Message addBodySection(Section bodySection);

    /**
     * Create and return a unmodifiable {@link Collection} that contains the {@link Section} instances currently
     * assigned to this message.  If there there is no body section(s) currently set an empty list is returned.
     *
     * @return an unmodifiable view of the currently set body section(s) or an empty collection if none set.
     */
    Collection<Section> getBodySections();

    /**
     * Performs the given action for each body {@link Section} of the {@link Message} until all sections
     * have been presented to the given {@link Consumer} or the consumer throws an exception.
     *
     * @param consumer
     *      the {@link Consumer} that will operate on each of the body sections in this message.
     *
     * @return this {@link Message} instance.
     */
    Message forEachBodySection(Consumer<Section> consumer);

}
