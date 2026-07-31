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
package org.apache.qpid.proton.message;

/**
 * Configuration options that can be passed to the {@link Message} decode APIs
 * to govern certain behaviours of the AMQP decoder.
 */
public final class MessageDecodeOptions {

    private static final int DEFAULT_MAX_DECODE_DEPTH = 32;

    private int _maxDecodeDepth = DEFAULT_MAX_DECODE_DEPTH;
    private int _zeroWidthElementLimit = 0;

    /**
     * New decode options with default values.
     */
    public MessageDecodeOptions() {
    }

    /**
     * Gets the configured maximum number of elements that can be decoded from an array
     * encoded with the zero-width elements (ulong0, uint0, list0, boolean-true,
     * boolean-false, null).
     *
     * @return the max number of zero-width array elements allowed.
     */
    public int getZeroWidthArrayElementLimit() {
        return _zeroWidthElementLimit;
    }

    /**
     * Configures the maximum number of array elements allowed while decoding arrays
     * of zero-width elements (ulong0, uint0, list0, boolean-true, boolean-false, null)
     * before a decode exception.
     *
     * Default is 0, meaning only the empty array is permitted.
     *
     * @param zeroWidthElementLimit The configured max zero-width elements allowed.
     *
     * @return this {@link MessageDecodeOptions} instance.
     */
    public MessageDecodeOptions setZeroWidthArrayElementLimit(int zeroWidthElementLimit) {
        _zeroWidthElementLimit = zeroWidthElementLimit;
        return this;
    }

    /**
     * Returns the maximum decode depth allowed before error when decoding Maps, Lists,
     * Arrays, and certain related DescribedTypes from an encoded payload.
     *
     * @return the maximum depth limit before error for Maps, Lists, Arrays and related DescribedTypes.
     */
    public int getMaxDecodeDepth() {
        return _maxDecodeDepth;
    }

    /**
     * Configures the maximum decode depth allowed before error when decoding Maps, Lists,
     * Arrays, and certain related DescribedTypes from an encoded payload. Default is 32.
     *
     * @param maxDecodeDepth The configured max decode depth allowed.
     *
     * @return this {@link MessageDecodeOptions} instance.
     */
    public MessageDecodeOptions setMaxDecodeDepth(int maxDecodeDepth) {
        this._maxDecodeDepth = maxDecodeDepth;
        return this;
    }
}