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
package org.apache.qpid.proton.codec;

public class ValidationSupport {

    private ValidationSupport()
    {
        // No instances
    }

    public static void validateMapSize(final int size, final int remaining)
    {
        if (size < 0)
        {
            throw new DecodeException("Map size " + size + " cannot be negative");
        }

        if (size > remaining)
        {
            throw new DecodeException("Map size " + size + " cannot be more than remaining bytes " + remaining);
        }
    }

    public static void validateMapCount(final int size, final int count, final int remaining)
    {
        if (count < 0)
        {
            throw new DecodeException("Map element count " + count + " cannot be negative");
        }

        if (count >= size)
        {
            throw new DecodeException("Map element count " + count + " must be less than size (" + size + ")");
        }

        if (count > remaining)
        {
            throw new DecodeException("Map element count " + count + " is specified to be greater than the amount of data available (" + remaining + ")");
        }

        if (count % 2 != 0)
        {
            throw new DecodeException("Map element count " + count + " cannot be odd");
        }
    }


    public static void validateListSize(final int size, final int remaining)
    {
        if (size < 0)
        {
            throw new DecodeException("List size " + size + " cannot be negative");
        }

        if (size > remaining)
        {
            throw new DecodeException("List size " + size + " cannot be more than remaining bytes " + remaining);
        }
    }

    public static void validateListCount(final int size, final int count, final int remaining)
    {
        if (count < 0)
        {
            throw new DecodeException("List element count " + count + " cannot be negative");
        }

        if (count >= size)
        {
            throw new DecodeException("List element count " + count + " must be less than size (" + size + ")");
        }

        if (count > remaining)
        {
            throw new DecodeException("List element count " + count + " is specified to be greater than the amount of data available (" + remaining + ")");
        }
    }

    public static void validateLength(final String type, final int length, final int remaining)
    {
        if (length < 0)
        {
            throw new DecodeException(type + " length " + length + " cannot be negative");
        }

        if (length > remaining)
        {
            throw new DecodeException(type + " length " + length + " cannot be more than remaining bytes " + remaining);
        }
    }
}
