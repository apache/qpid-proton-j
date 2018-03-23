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
package org.apache.qpid.proton;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * IDE Runner for python based tests by loading test resources from the mvn configured base
 * directory
 */
@Ignore
public class JythonIDETest extends JythonTest {

    @Before
    public void setUp() {
        System.setProperty(PROTON_JYTHON_BINDING, "java/shim/binding");
        System.setProperty(PROTON_JYTHON_SHIM, "java/shim");
        System.setProperty(PROTON_JYTHON_TEST_ROOT, "python/");
        System.setProperty(PROTON_JYTHON_TEST_SCRIPT, "python/proton-test");
        System.setProperty(PROTON_JYTHON_IGNORE_FILE, "java/pythonTests.ignore");
    }

    @After
    public void tearDown() {
        System.clearProperty(PROTON_JYTHON_BINDING);
        System.clearProperty(PROTON_JYTHON_SHIM);
        System.clearProperty(PROTON_JYTHON_TEST_ROOT);
        System.clearProperty(PROTON_JYTHON_TEST_SCRIPT);
        System.clearProperty(TEST_PATTERN_SYSTEM_PROPERTY);
    }

    @Ignore
    @Test
    public void test() {}

    @Test
    public void testConnectionTest() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.ConnectionTest.*");
        super.test();
    }

    @Test
    public void testSessionTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.SessionTest.*");
        super.test();
    }

    @Test
    public void testTransferTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.TransferTest.*");
        super.test();
    }

    @Test
    public void testSettlementTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.SettlementTest.*");
        super.test();
    }

    @Test
    public void testSessionCreditTest() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.SessionCreditTest.*");
        super.test();
    }

    @Test
    public void testLinkTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.LinkTest.*");
        super.test();
    }

    @Test
    public void testEventTest() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.EventTest.*");
        super.test();
    }

    @Test
    public void testDeliveryTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.DeliveryTest.*");
        super.test();
    }

    @Test
    public void testCreditTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.CreditTest.*");
        super.test();
    }

    @Test
    public void testCodecDataTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.codec.DataTest.*");
        super.test();
    }

    @Test
    public void testInteropTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.interop.InteropTest.*");
        super.test();
    }

    @Test
    public void testClientTransportTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.transport.ClientTransportTest.*");
        super.test();
    }

    @Test
    public void testMaxFrameTransferTests() throws Exception {
        System.setProperty(TEST_PATTERN_SYSTEM_PROPERTY, "proton_tests.engine.MaxFrameTransferTest.*");
        super.test();
    }
}
