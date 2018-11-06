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

import javax.net.ssl.SSLContext;
import org.apache.qpid.proton.engine.ProtonJSslDomain;
import org.apache.qpid.proton.engine.SslDomain;
import org.apache.qpid.proton.engine.SslPeerDetails;

public class SslDomainImpl implements SslDomain, ProtonSslEngineProvider, ProtonJSslDomain
{
    private Mode _mode;
    private VerifyMode _verifyMode;
    private String _certificateFile;
    private String _privateKeyFile;
    private String _privateKeyPassword;
    private String _trustedCaDb;
    private boolean _allowUnsecuredClient;
    private SSLContext _sslContext;

    private final SslEngineFacadeFactory _sslEngineFacadeFactory = new SslEngineFacadeFactory();

    /**
     * Application code should use {@link org.apache.qpid.proton.engine.SslDomain.Factory#create()} instead.
     */
    public SslDomainImpl()
    {
    }

    @Override
    public void init(Mode mode)
    {
        _sslEngineFacadeFactory.resetCache();
        _mode = mode;
    }

    @Override
    public Mode getMode()
    {
        return _mode;
    }

    @Override
    public void setCredentials(String certificateFile, String privateKeyFile, String privateKeyPassword)
    {
        _certificateFile = certificateFile;
        _privateKeyFile = privateKeyFile;
        _privateKeyPassword = privateKeyPassword;
        _sslEngineFacadeFactory.resetCache();
    }

    @Override
    public void setTrustedCaDb(String certificateDb)
    {
        _trustedCaDb = certificateDb;
        _sslEngineFacadeFactory.resetCache();
    }

    @Override
    public String getTrustedCaDb()
    {
        return _trustedCaDb;
    }

    @Override
    public void setSslContext(SSLContext sslContext)
    {
        _sslContext = sslContext;
    }

    @Override
    public SSLContext getSslContext()
    {
        return _sslContext;
    }

    @Override
    public void setPeerAuthentication(VerifyMode verifyMode)
    {
        _verifyMode = verifyMode;
        _sslEngineFacadeFactory.resetCache();
    }

    @Override
    public VerifyMode getPeerAuthentication()
    {
        if(_verifyMode == null)
        {
           return _mode == Mode.SERVER ? VerifyMode.ANONYMOUS_PEER : VerifyMode.VERIFY_PEER_NAME;
        }

        return _verifyMode;
    }

    @Override
    public String getPrivateKeyFile()
    {
        return _privateKeyFile;
    }

    @Override
    public String getPrivateKeyPassword()
    {
        return _privateKeyPassword;
    }

    @Override
    public String getCertificateFile()
    {
        return _certificateFile;
    }

    @Override
    public void allowUnsecuredClient(boolean allowUnsecured)
    {
        _allowUnsecuredClient = allowUnsecured;
        _sslEngineFacadeFactory.resetCache();
    }

    @Override
    public boolean allowUnsecuredClient()
    {
        return _allowUnsecuredClient;
    }

    @Override
    public ProtonSslEngine createSslEngine(SslPeerDetails peerDetails)
    {
        return _sslEngineFacadeFactory.createProtonSslEngine(this, peerDetails);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SslDomainImpl [_mode=").append(_mode)
            .append(", _verifyMode=").append(_verifyMode)
            .append(", _certificateFile=").append(_certificateFile)
            .append(", _privateKeyFile=").append(_privateKeyFile)
            .append(", _trustedCaDb=").append(_trustedCaDb)
            .append(", _allowUnsecuredClient=").append(_allowUnsecuredClient)
            .append("]");
        return builder.toString();
    }
}
