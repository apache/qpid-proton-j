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
package org.apache.qpid.proton.reactor;

public class ReactorOptions {
    private boolean enableSaslByDefault = true;
    private int maxFrameSize;

    /**
     * Sets whether SASL will be automatically enabled with ANONYMOUS as the mechanism,
     * or if no SASL layer will be enabled. If disabled, the application handlers
     * can still enable SASL on the transport when it is bound to the connection.
     *
     * True by default.
     *
     * @param enableSaslByDefault
     *            true if SASL should be enabled by default, false if not.
     */
    public void setEnableSaslByDefault(boolean enableSaslByDefault) {
        this.enableSaslByDefault = enableSaslByDefault;
    }

    /**
     * Returns whether SASL should be enabled by default.
     *
     * @return True if SASL should be enabled by default, false if not.
     * @see #setEnableSaslByDefault(boolean)
     */
    public boolean isEnableSaslByDefault() {
        return this.enableSaslByDefault;
    }

    /**
     * Sets the maximum frame size value to announce in the Open frame.
     *
     * @param maxFrameSize The frame size in bytes.
     */
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    /**
     * Gets the maximum frame size to be announced in the Open frame.
     *
     * @return the frame size in bytes or 0 if none is set.
     * @see #setMaxFrameSize(int)
     */
    public int getMaxFrameSize() {
      return maxFrameSize;
    }
}
