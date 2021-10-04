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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EncodingCodesTest {

    @Test
    public void testEncodingCodeLabels() {
        doEncodingCodeLabelTestImpl((byte) 0x00, EncodingCodes.DESCRIBED_TYPE_INDICATOR, "DESCRIBED_TYPE_INDICATOR");

        doEncodingCodeLabelTestImpl((byte) 0x40, EncodingCodes.NULL, "NULL");

        doEncodingCodeLabelTestImpl((byte) 0x56, EncodingCodes.BOOLEAN, "BOOLEAN");
        doEncodingCodeLabelTestImpl((byte) 0x41, EncodingCodes.BOOLEAN_TRUE, "BOOLEAN_TRUE");
        doEncodingCodeLabelTestImpl((byte) 0x42, EncodingCodes.BOOLEAN_FALSE, "BOOLEAN_FALSE");

        doEncodingCodeLabelTestImpl((byte) 0x50, EncodingCodes.UBYTE, "UBYTE");

        doEncodingCodeLabelTestImpl((byte) 0x60, EncodingCodes.USHORT, "USHORT");

        doEncodingCodeLabelTestImpl((byte) 0x70, EncodingCodes.UINT, "UINT");
        doEncodingCodeLabelTestImpl((byte) 0x52, EncodingCodes.SMALLUINT, "SMALLUINT");
        doEncodingCodeLabelTestImpl((byte) 0x43, EncodingCodes.UINT0, "UINT0");

        doEncodingCodeLabelTestImpl((byte) 0x80, EncodingCodes.ULONG, "ULONG");
        doEncodingCodeLabelTestImpl((byte) 0x53, EncodingCodes.SMALLULONG, "SMALLULONG");
        doEncodingCodeLabelTestImpl((byte) 0x44, EncodingCodes.ULONG0, "ULONG0");

        doEncodingCodeLabelTestImpl((byte) 0x51, EncodingCodes.BYTE, "BYTE");

        doEncodingCodeLabelTestImpl((byte) 0x61, EncodingCodes.SHORT, "SHORT");

        doEncodingCodeLabelTestImpl((byte) 0x71, EncodingCodes.INT, "INT");
        doEncodingCodeLabelTestImpl((byte) 0x54, EncodingCodes.SMALLINT, "SMALLINT");

        doEncodingCodeLabelTestImpl((byte) 0x81, EncodingCodes.LONG, "LONG");
        doEncodingCodeLabelTestImpl((byte) 0x55, EncodingCodes.SMALLLONG, "SMALLLONG");

        doEncodingCodeLabelTestImpl((byte) 0x72, EncodingCodes.FLOAT, "FLOAT");

        doEncodingCodeLabelTestImpl((byte) 0x82, EncodingCodes.DOUBLE, "DOUBLE");

        doEncodingCodeLabelTestImpl((byte) 0x74, EncodingCodes.DECIMAL32, "DECIMAL32");

        doEncodingCodeLabelTestImpl((byte) 0x84, EncodingCodes.DECIMAL64, "DECIMAL64");

        doEncodingCodeLabelTestImpl((byte) 0x94, EncodingCodes.DECIMAL128, "DECIMAL128");

        doEncodingCodeLabelTestImpl((byte) 0x73, EncodingCodes.CHAR, "CHAR");

        doEncodingCodeLabelTestImpl((byte) 0x83, EncodingCodes.TIMESTAMP, "TIMESTAMP");

        doEncodingCodeLabelTestImpl((byte) 0x98, EncodingCodes.UUID, "UUID");

        doEncodingCodeLabelTestImpl((byte) 0xa0, EncodingCodes.VBIN8, "VBIN8");
        doEncodingCodeLabelTestImpl((byte) 0xb0, EncodingCodes.VBIN32, "VBIN32");

        doEncodingCodeLabelTestImpl((byte) 0xa1, EncodingCodes.STR8, "STR8");
        doEncodingCodeLabelTestImpl((byte) 0xb1, EncodingCodes.STR32, "STR32");

        doEncodingCodeLabelTestImpl((byte) 0xa3, EncodingCodes.SYM8, "SYM8");
        doEncodingCodeLabelTestImpl((byte) 0xb3, EncodingCodes.SYM32, "SYM32");

        doEncodingCodeLabelTestImpl((byte) 0x45, EncodingCodes.LIST0, "LIST0");
        doEncodingCodeLabelTestImpl((byte) 0xc0, EncodingCodes.LIST8, "LIST8");
        doEncodingCodeLabelTestImpl((byte) 0xd0, EncodingCodes.LIST32, "LIST32");

        doEncodingCodeLabelTestImpl((byte) 0xc1, EncodingCodes.MAP8, "MAP8");
        doEncodingCodeLabelTestImpl((byte) 0xd1, EncodingCodes.MAP32, "MAP32");

        doEncodingCodeLabelTestImpl((byte) 0xe0, EncodingCodes.ARRAY8, "ARRAY8");
        doEncodingCodeLabelTestImpl((byte) 0xf0, EncodingCodes.ARRAY32, "ARRAY32");

        assertEquals("Unknown-Type:0xEE", EncodingCodes.toString((byte) 0xEE));
    }

    public void doEncodingCodeLabelTestImpl(byte expectedCode, byte actualCode, String name) {
        assertEquals("Unexpected constant value for " + name, expectedCode, actualCode);
        assertEquals(name + ":0x" + String.format("%02x", expectedCode), EncodingCodes.toString(actualCode));
    }
}
