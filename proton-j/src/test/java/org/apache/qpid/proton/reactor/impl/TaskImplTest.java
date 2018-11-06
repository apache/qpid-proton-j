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
package org.apache.qpid.proton.reactor.impl;

import org.junit.Assert;
import org.junit.Test;

public class TaskImplTest {

    @Test
    public void testCompareToWithSameObject() {
        TaskImpl task = new TaskImpl(1000, 1);
        Assert.assertEquals(0, task.compareTo(task));
    }

    @Test
    public void testCompareToWithDifferentDeadlines() {
        TaskImpl task1 = new TaskImpl(1000, 1);
        TaskImpl task2 = new TaskImpl(2000, 2);

        Assert.assertTrue(task1.compareTo(task2) < 0);
        Assert.assertTrue(task2.compareTo(task1) > 0);
    }

    @Test
    public void testCompareToWithSameDeadlines() {
        int deadline = 1000;
        TaskImpl task1 = new TaskImpl(deadline, 1);
        TaskImpl task2 = new TaskImpl(deadline, 2);

        Assert.assertTrue("Expected task1 to order 'less' due to being created first", task1.compareTo(task2) < 0);
        Assert.assertTrue("Expected task2 to order 'greater' due to being created second", task1.compareTo(task2) < 0);
        Assert.assertTrue(task2.compareTo(task1) > 0);
    }
}
