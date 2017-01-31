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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.qpid.proton.reactor.Reactor;
import org.junit.Test;

public class EventDelegationTest {

    private ArrayList<String> trace = new ArrayList<String>();

    class ExecutionFlowTracer extends BaseHandler {
        protected String name;

        ExecutionFlowTracer(String name) {
            this.name = name;
        }

        @Override
        public void onReactorInit(Event e) {
            trace.add(name);
        }
    }

    class DelegatingFlowTracer extends ExecutionFlowTracer {
        public DelegatingFlowTracer(String name) {
            super(name);
        }

        @Override
        public void onReactorInit(Event e) {
            trace.add("(" + name);
            e.delegate();
            trace.add(name + ")");
        }
    }

    Handler assemble(Handler outer, Handler...inner) {
        for(Handler h : inner) {
            outer.add(h);
        }
        return outer;
    }

    @Test
    public void testImplicitDelegate() throws IOException {
        Handler h = 
                assemble(
                        new ExecutionFlowTracer("A"),
                        assemble(
                                new ExecutionFlowTracer("A.A"),
                                new ExecutionFlowTracer("A.A.A"),
                                new ExecutionFlowTracer("A.A.B")
                                ),
                        assemble(
                                new ExecutionFlowTracer("A.B")
                                )
                );
        Reactor r = Reactor.Factory.create();
        r.getHandler().add(h);
        r.run();
        assertArrayEquals(new String[]{"A", "A.A", "A.A.A", "A.A.B", "A.B"}, trace.toArray());
    }

    @Test
    public void testExplicitDelegate() throws IOException {
        Handler h = 
                assemble(
                        new ExecutionFlowTracer("A"),
                        assemble(
                                new DelegatingFlowTracer("A.A"),
                                new ExecutionFlowTracer("A.A.A"),
                                new ExecutionFlowTracer("A.A.B")
                                ),
                        assemble(
                                new ExecutionFlowTracer("A.B")
                                )
                );
        Reactor r = Reactor.Factory.create();
        r.getHandler().add(h);
        r.run();
        assertArrayEquals(new String[]{"A", "(A.A", "A.A.A", "A.A.B", "A.A)", "A.B"}, trace.toArray());
    }

}
