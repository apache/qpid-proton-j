/*
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
 */
package org.apache.qpid.proton.engine.impl.ssl;

import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;

import org.apache.qpid.proton.engine.SslDomain;
import org.apache.qpid.proton.engine.SslPeerDetails;
import org.junit.Test;
import static org.mockito.Mockito.mock;
public class SslEngineFacadeFactoryTest {

    private static final String PASSWORD = "unittest";

    @Test
    public void testCertifcateLoad() {
        String ipFile = resolveFilename("cert.pem.txt");
        SslEngineFacadeFactory factory = new SslEngineFacadeFactory();

        assertNotNull("Certificate was NULL", factory.readCertificate(ipFile));
    }

    @Test
    public void testLoadKey() {
        String keyFile = resolveFilename("key.pem.txt");
        SslEngineFacadeFactory factory = new SslEngineFacadeFactory();

        assertNotNull("Key was NULL", factory.readPrivateKey(keyFile, PASSWORD));
    }

    @Test
    public void testLoadUnencryptedPrivateKey(){
        String keyFile = resolveFilename("private-key-clear.pem.txt");
        SslEngineFacadeFactory factory = new SslEngineFacadeFactory();

        assertNotNull("Key was NULL", factory.readPrivateKey(keyFile, null));
    }

    @Test
    public void testLoadUnencryptedPKCS8PrivateKey(){
        String keyFile = resolveFilename("private-key-clear-pkcs8.pem.txt");
        SslEngineFacadeFactory factory = new SslEngineFacadeFactory();

        assertNotNull("Key was NULL", factory.readPrivateKey(keyFile, null));
    }
    
    @Test
    public void testSSLContext(){
        String certFile = resolveFilename("cert.pem.txt");
        String keyFile = resolveFilename("private-key-clear.pem.txt");
        //sslDomain object
        SslDomain domain=SslDomain.Factory.create();
        domain.setCredentials(certFile, keyFile, null);
        //Dummy SSLContext
        DummySslContextFactory dummySSLContext = new DummySslContextFactory();
	    SSLContext sslContext=dummySSLContext.getOrCreateSslContext(domain);
	    //set SSLContext in SslDomainObject
	    domain.setSslcontext(sslContext);
	    
	    SslEngineFacadeFactory factory = new SslEngineFacadeFactory();
	    //Ensure sslEngine is created with the provided SSLContext
	    ProtonSslEngine sslEngine= factory.createProtonSslEngine(domain,  SslPeerDetails.Factory.create("127.0.0.1",1));
		
	    //Verify sslEngine is created with provided SSLContext
	    assertNotNull("Cant create SSLEngine with provided Context", sslEngine);
	}

    private String resolveFilename(String testFilename) {
        URL resourceUri = this.getClass().getResource(testFilename);

        assertNotNull("Failed to load file: " + testFilename, resourceUri);

        String fName = resourceUri.getPath();

        return fName;
    }
}
