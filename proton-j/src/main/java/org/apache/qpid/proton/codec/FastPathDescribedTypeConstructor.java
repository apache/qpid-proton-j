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
package org.apache.qpid.proton.codec;

/**
 * Marker interface that indicates the TypeConstructor can decode known Proton-J types
 * using a fast path read / write operation.  These types may result in an encode that
 * does not always write the smallest form of the given type to save time.
 *
 * @param <V> The type that this constructor handles
 */
public interface FastPathDescribedTypeConstructor<V> extends TypeConstructor<V> {

}
