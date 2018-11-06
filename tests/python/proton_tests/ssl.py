from __future__ import absolute_import
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
from . import common
import random
import string
import subprocess
import sys
from proton import *
from org.apache.qpid.proton.engine import TransportException
from .common import Skipped, pump


def _testpath(file):
    """ Set the full path to the certificate,keyfile, etc. for the test.
    """
    if os.name=="nt":
        if file.find("private-key")!=-1:
            # The private key is not in a separate store
            return None
        # Substitute pkcs#12 equivalent for the CA/key store
        if file.endswith(".pem"):
            file = file[:-4] + ".p12"
    return os.path.join(os.path.dirname(__file__),
                        "ssl_db/%s" % file)

class SslTest(common.Test):

    def __init__(self, *args):
        common.Test.__init__(self, *args)
        self._testpath = _testpath

    def setUp(self):
        if not common.isSSLPresent():
            raise Skipped("No SSL libraries found.")
        self.server_domain = SSLDomain(SSLDomain.MODE_SERVER)
        self.client_domain = SSLDomain(SSLDomain.MODE_CLIENT)

    def tearDown(self):
        self.server_domain = None
        self.client_domain = None

    class SslTestConnection(object):
        """ Represents a single SSL connection.
        """
        def __init__(self, domain=None, mode=Transport.CLIENT,
                     session_details=None, conn_hostname=None,
                     ssl_peername=None):
            if not common.isSSLPresent():
                raise Skipped("No SSL libraries found.")

            self.ssl = None
            self.domain = domain
            self.transport = Transport(mode)
            self.connection = Connection()
            if conn_hostname:
                self.connection.hostname = conn_hostname
            if domain:
                self.ssl = SSL( self.transport, self.domain, session_details )
                if ssl_peername:
                    self.ssl.peer_hostname = ssl_peername
            # bind last, after all configuration complete:
            self.transport.bind(self.connection)

    def _pump(self, ssl_client, ssl_server, buffer_size=1024):
        pump(ssl_client.transport, ssl_server.transport, buffer_size)

    def _pump_with_failing_negotiation(self, client, server, onesided = False):
        # Exception once for client/server transport
        try:
            self._pump( client, server )
            assert False, "Expected exception did not occur!"
        except TransportException:
            pass

        if(onesided != True):
            # Exception once for server/client transport
            try:
                self._pump( client, server )
                assert False, "Expected exception did not occur!"
            except TransportException:
                pass

        # Ensure both are processed to completion
        self._pump( client, server )

    def _do_handshake(self, client, server):
        """ Attempt to connect client to server. Will throw a TransportException if the SSL
        handshake fails.
        """
        client.connection.open()
        server.connection.open()
        self._pump(client, server)
        if client.transport.closed:
            return
        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump(client, server)

    def test_defaults(self):
        if os.name=="nt":
            raise Skipped("Windows SChannel lacks anonymous cipher support.")
        """ By default, both the server and the client support anonymous
        ciphers - they should connect without need for a certificate.
        """
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )
        client = SslTest.SslTestConnection( self.client_domain )

        # check that no SSL connection exists
        assert not server.ssl.cipher_name()
        assert not client.ssl.protocol_name()

        #client.transport.trace(Transport.TRACE_DRV)
        #server.transport.trace(Transport.TRACE_DRV)

        client.connection.open()
        server.connection.open()
        self._pump( client, server )

        # now SSL should be active
        assert server.ssl.cipher_name() is not None
        assert client.ssl.protocol_name() is not None

        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_ssl_with_small_buffer(self):
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()

        small_buffer_size = 1
        self._pump( client, server, small_buffer_size )

        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )


    def test_server_certificate(self):
        """ Test that anonymous clients can still connect to a server that has
        a certificate configured.
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        self.client_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_server_authentication(self):
        """ Simple SSL connection with authentication of the server
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")

        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_client_authentication(self):
        """ Force the client to authenticate.
        """
        # note: when requesting client auth, the server _must_ send its
        # certificate, so make sure we configure one!
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        # give the client a certificate, but let's not require server authentication
        self.client_domain.set_credentials(self._testpath("client-certificate.pem"),
                                           self._testpath("client-private-key.pem"),
                                           "client-password")
        self.client_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )

        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_client_authentication_fail_bad_cert(self):
        """ Ensure that the server can detect a bad client certificate.
        """
        # note: when requesting client auth, the server _must_ send its
        # certificate, so make sure we configure one!
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        self.client_domain.set_credentials(self._testpath("bad-server-certificate.pem"),
                                           self._testpath("bad-server-private-key.pem"),
                                           "server-password")
        self.client_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump_with_failing_negotiation(client, server)
        assert client.transport.closed
        assert server.transport.closed
        assert client.connection.state & Endpoint.REMOTE_UNINIT
        assert server.connection.state & Endpoint.REMOTE_UNINIT

    def test_client_authentication_fail_no_cert(self):
        """ Ensure that the server will fail a client that does not provide a
        certificate.
        """
        # note: when requesting client auth, the server _must_ send its
        # certificate, so make sure we configure one!
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        self.client_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump_with_failing_negotiation(client, server)
        assert client.transport.closed
        assert server.transport.closed
        assert client.connection.state & Endpoint.REMOTE_UNINIT
        assert server.connection.state & Endpoint.REMOTE_UNINIT

    def test_client_server_authentication(self):
        """ Require both client and server to mutually identify themselves.
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )

        self.client_domain.set_credentials(self._testpath("client-certificate.pem"),
                                           self._testpath("client-private-key.pem"),
                                           "client-password")
        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_server_only_authentication(self):
        """ Client verifies server, but server does not verify client.
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )

        self.client_domain.set_credentials(self._testpath("client-certificate.pem"),
                                           self._testpath("client-private-key.pem"),
                                           "client-password")
        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_bad_server_certificate(self):
        """ A server with a self-signed certificate that is not trusted by the
        client.  The client should reject the server.
        """
        self.server_domain.set_credentials(self._testpath("bad-server-certificate.pem"),
                                           self._testpath("bad-server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )

        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )
        client = SslTest.SslTestConnection( self.client_domain )

        client.connection.open()
        server.connection.open()
        self._pump_with_failing_negotiation(client, server)
        assert client.transport.closed
        assert server.transport.closed
        assert client.connection.state & Endpoint.REMOTE_UNINIT
        assert server.connection.state & Endpoint.REMOTE_UNINIT

        del server
        del client

        # now re-try with a client that does not require peer verification
        self.client_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )

        client = SslTest.SslTestConnection( self.client_domain )
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert client.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_allow_unsecured_client_which_connects_unsecured(self):
        """ Server allows an unsecured client to connect if configured.
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )
        # allow unsecured clients on this connection
        self.server_domain.allow_unsecured_client()
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        # non-ssl connection
        client = SslTest.SslTestConnection()

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert server.ssl.protocol_name() is None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )

    def test_allow_unsecured_client_which_connects_secured(self):
        """ As per test_allow_unsecured_client_which_connects_unsecured
            but client actually uses SSL
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )

        self.client_domain.set_credentials(self._testpath("client-certificate.pem"),
                                           self._testpath("client-private-key.pem"),
                                           "client-password")
        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        # allow unsecured clients on this connection
        #self.server_domain.allow_unsecured_client()

        # client uses ssl. Server should detect this.
        client = SslTest.SslTestConnection( self.client_domain )
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        client.connection.open()
        server.connection.open()
        self._pump( client, server )
        assert server.ssl.protocol_name() is not None
        client.connection.close()
        server.connection.close()
        self._pump( client, server )


    def test_disallow_unsecured_client(self):
        """ Non-SSL Client is disallowed from connecting to server.
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.ANONYMOUS_PEER )
        server = SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER )

        # non-ssl connection
        client = SslTest.SslTestConnection()

        client.connection.open()
        server.connection.open()
        self._pump_with_failing_negotiation(client, server, onesided = True)
        assert client.transport.closed
        assert server.transport.closed
        assert client.connection.state & Endpoint.REMOTE_UNINIT
        assert server.connection.state & Endpoint.REMOTE_UNINIT


    def test_multiple_sessions(self):
        """ Test multiple simultaineous active SSL sessions with bi-directional
        certificate verification, shared across two domains.
        """
        self.server_domain.set_credentials(self._testpath("server-certificate.pem"),
                                           self._testpath("server-private-key.pem"),
                                           "server-password")
        self.server_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.server_domain.set_peer_authentication( SSLDomain.VERIFY_PEER,
                                                    self._testpath("ca-certificate.pem") )

        self.client_domain.set_credentials(self._testpath("client-certificate.pem"),
                                           self._testpath("client-private-key.pem"),
                                           "client-password")
        self.client_domain.set_trusted_ca_db(self._testpath("ca-certificate.pem"))
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )

        max_count = 100
        sessions = [(SslTest.SslTestConnection( self.server_domain, mode=Transport.SERVER ),
                     SslTest.SslTestConnection( self.client_domain )) for x in
                    range(max_count)]
        for s in sessions:
            s[0].connection.open()
            self._pump( s[0], s[1] )

        for s in sessions:
            s[1].connection.open()
            self._pump( s[1], s[0] )
            assert s[0].ssl.cipher_name() is not None
            assert s[1].ssl.cipher_name() == s[0].ssl.cipher_name()

        for s in sessions:
            s[1].connection.close()
            self._pump( s[0], s[1] )

        for s in sessions:
            s[0].connection.close()
            self._pump( s[1], s[0] )

    def test_singleton(self):
        """Verify that only a single instance of SSL can exist per Transport"""
        transport = Transport()
        self.client_domain.set_peer_authentication( SSLDomain.VERIFY_PEER )
        ssl1 = SSL(transport, self.client_domain)
        ssl2 = transport.ssl(self.client_domain)
        ssl3 = transport.ssl(self.client_domain)
        assert ssl1 is ssl2
        assert ssl1 is ssl3
        transport = Transport()
        ssl1 = transport.ssl(self.client_domain)
        ssl2 = SSL(transport, self.client_domain)
        assert ssl1 is ssl2
        # catch attempt to re-configure existing SSL
        try:
            ssl3 = SSL(transport, self.server_domain)
            assert False, "Expected error did not occur!"
        except SSLException:
            pass

