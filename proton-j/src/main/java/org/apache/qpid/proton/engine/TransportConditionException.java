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

import java.util.Objects;

import org.apache.qpid.proton.amqp.Symbol;

public class TransportConditionException extends TransportException {

    private static final long serialVersionUID = 828725563575892233L;
    private Symbol _condition;

    public TransportConditionException(String message, Symbol condition) {
        this(message, condition, null);
    }

    public TransportConditionException(String message, Symbol condition, Throwable cause) {
        super(message, cause);
        Objects.requireNonNull(message, "A message must be provided");
        Objects.requireNonNull(condition, "A condition must be provided");
        _condition = condition;
    }

    public Symbol getCondition() {
        return _condition;
    }
}
