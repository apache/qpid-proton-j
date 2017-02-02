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
from __future__ import absolute_import

import sys, os
from . import common
from . import engine

from proton import *
from .common import pump, Skipped
from proton._compat import str2bin

def _sslCertpath(file):
    """ Return the full path to the certificate,keyfile, etc.
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

def _testSaslMech(self, mech, clientUser='user@proton', authUser='user@proton', encrypted=False, authenticated=True):
  self.s1.allowed_mechs(mech)
  self.c1.open()
  self.c2.open()

  pump(self.t1, self.t2, 1024)

  if encrypted is not None:
    assert self.t2.encrypted == encrypted, encrypted
    assert self.t1.encrypted == encrypted, encrypted

  assert self.t2.authenticated == authenticated, authenticated
  assert self.t1.authenticated == authenticated, authenticated
  if authenticated:
    # Server
    assert self.t2.user == authUser
    assert self.s2.user == authUser
    assert self.s2.mech == mech.strip()
    assert self.s2.outcome == SASL.OK, self.s2.outcome
    assert self.c2.state & Endpoint.LOCAL_ACTIVE and self.c2.state & Endpoint.REMOTE_ACTIVE,\
      "local_active=%s, remote_active=%s" % (self.c1.state & Endpoint.LOCAL_ACTIVE, self.c1.state & Endpoint.REMOTE_ACTIVE)
    # Client
    assert self.t1.user == clientUser
    assert self.s1.user == clientUser
    assert self.s1.mech == mech.strip()
    assert self.s1.outcome == SASL.OK, self.s1.outcome
    assert self.c1.state & Endpoint.LOCAL_ACTIVE and self.c1.state & Endpoint.REMOTE_ACTIVE,\
      "local_active=%s, remote_active=%s" % (self.c1.state & Endpoint.LOCAL_ACTIVE, self.c1.state & Endpoint.REMOTE_ACTIVE)
  else:
    # Server
    assert self.t2.user == None
    assert self.s2.user == None
    assert self.s2.outcome != SASL.OK, self.s2.outcome
    # Client
    assert self.t1.user == clientUser
    assert self.s1.user == clientUser
    assert self.s1.outcome != SASL.OK, self.s1.outcome

class Test(common.Test):
  pass

def consumeAllOuput(t):
  stops = 0
  while stops<1:
    out = t.peek(1024)
    l = len(out) if out else 0
    t.pop(l)
    if l <= 0:
      stops += 1

class SaslTest(Test):

  def setUp(self):
    self.t1 = Transport()
    self.s1 = SASL(self.t1)
    self.t2 = Transport(Transport.SERVER)
    self.t2.max_frame_size = 65536
    self.s2 = SASL(self.t2)

  def pump(self):
    pump(self.t1, self.t2, 1024)

  def testPipelinedClient(self):
    # TODO: When PROTON-1136 is fixed then remove this test
    if "java" in sys.platform:
      raise Skipped("Proton-J does not support pipelined client input")

    # Server
    self.s2.allowed_mechs('ANONYMOUS')

    c2 = Connection()
    self.t2.bind(c2)

    assert self.s2.outcome is None

    # Push client bytes into server
    self.t2.push(str2bin(
        # SASL
        'AMQP\x03\x01\x00\x00'
        # @sasl-init(65) [mechanism=:ANONYMOUS, initial-response=b"anonymous@fuschia"]
        '\x00\x00\x002\x02\x01\x00\x00\x00SA\xd0\x00\x00\x00"\x00\x00\x00\x02\xa3\x09ANONYMOUS\xa0\x11anonymous@fuschia'
        # AMQP
        'AMQP\x00\x01\x00\x00'
        # @open(16) [container-id="", channel-max=1234]
        '\x00\x00\x00!\x02\x00\x00\x00\x00S\x10\xd0\x00\x00\x00\x11\x00\x00\x00\x0a\xa1\x00@@`\x04\xd2@@@@@@'
        ))

    consumeAllOuput(self.t2)

    assert not self.t2.condition
    assert self.s2.outcome == SASL.OK
    assert c2.state & Endpoint.REMOTE_ACTIVE

  def testPipelinedServer(self):
    # Client
    self.s1.allowed_mechs('ANONYMOUS')

    c1 = Connection()
    self.t1.bind(c1)

    assert self.s1.outcome is None

    # Push server bytes into client
    # Commented out lines in this test are where the client input processing doesn't
    # run after output processing even though there is input waiting
    self.t1.push(str2bin(
        # SASL
        'AMQP\x03\x01\x00\x00'
        # @sasl-mechanisms(64) [sasl-server-mechanisms=@PN_SYMBOL[:ANONYMOUS]]
        '\x00\x00\x00\x1c\x02\x01\x00\x00\x00S@\xc0\x0f\x01\xe0\x0c\x01\xa3\tANONYMOUS'
        # @sasl-outcome(68) [code=0]
        '\x00\x00\x00\x10\x02\x01\x00\x00\x00SD\xc0\x03\x01P\x00'
        # AMQP
        'AMQP\x00\x01\x00\x00'
        # @open(16) [container-id="", channel-max=1234]
        '\x00\x00\x00!\x02\x00\x00\x00\x00S\x10\xd0\x00\x00\x00\x11\x00\x00\x00\x0a\xa1\x00@@`\x04\xd2@@@@@@'
        ))

    consumeAllOuput(self.t1)

    assert self.s1.outcome == SASL.OK
    assert c1.state & Endpoint.REMOTE_ACTIVE

  def testPipelined2(self):
    if "java" in sys.platform:
      raise Skipped("Proton-J does not support client pipelining")

    out1 = self.t1.peek(1024)
    self.t1.pop(len(out1))
    self.t2.push(out1)

    self.s2.allowed_mechs('ANONYMOUS')
    c2 = Connection()
    c2.open()
    self.t2.bind(c2)

    out2 = self.t2.peek(1024)
    self.t2.pop(len(out2))
    self.t1.push(out2)

    out1 = self.t1.peek(1024)
    assert len(out1) > 0

  def testFracturedSASL(self):
    """ PROTON-235
    """
    assert self.s1.outcome is None

    # self.t1.trace(Transport.TRACE_FRM)

    out = self.t1.peek(1024)
    self.t1.pop(len(out))
    self.t1.push(str2bin("AMQP\x03\x01\x00\x00"))
    out = self.t1.peek(1024)
    self.t1.pop(len(out))
    self.t1.push(str2bin("\x00\x00\x00"))
    out = self.t1.peek(1024)
    self.t1.pop(len(out))

    self.t1.push(str2bin("6\x02\x01\x00\x00\x00S@\xc04\x01\xe01\x04\xa3\x05PLAIN\x0aDIGEST-MD5\x09ANONYMOUS\x08CRAM-MD5"))
    out = self.t1.peek(1024)
    self.t1.pop(len(out))
    self.t1.push(str2bin("\x00\x00\x00\x10\x02\x01\x00\x00\x00SD\xc0\x03\x01P\x00"))
    out = self.t1.peek(1024)
    self.t1.pop(len(out))
    while out:
      out = self.t1.peek(1024)
      self.t1.pop(len(out))

    assert self.s1.outcome == SASL.OK, self.s1.outcome

  def test_singleton(self):
      """Verify that only a single instance of SASL can exist per Transport"""
      transport = Transport()
      attr = object()
      sasl1 = SASL(transport)
      sasl1.my_attribute = attr
      sasl2 = transport.sasl()
      sasl3 = SASL(transport)
      assert sasl1 == sasl2
      assert sasl1 == sasl3
      assert sasl1.my_attribute == attr
      assert sasl2.my_attribute == attr
      assert sasl3.my_attribute == attr
      transport = Transport()
      sasl1 = transport.sasl()
      sasl1.my_attribute = attr
      sasl2 = SASL(transport)
      assert sasl1 == sasl2
      assert sasl1.my_attribute == attr
      assert sasl2.my_attribute == attr
