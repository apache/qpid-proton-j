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

from unittest import TestCase
try:
  from unittest import SkipTest
except:
  try:
    from unittest2 import SkipTest
  except:
    class SkipTest(Exception):
      pass

from random import randint
from threading import Thread
from socket import socket, AF_INET, SOCK_STREAM
from subprocess import Popen,PIPE,STDOUT
import sys, os, string, subprocess
from proton import Connection, Transport, SASL, Endpoint, Delivery, SSL
from proton.reactor import Container
from proton.handlers import CHandshaker, CFlowController
from string import Template

if sys.version_info[0] == 2 and sys.version_info[1] < 6:
    # this is for compatibility, apparently the version of jython we
    # use doesn't have the next() builtin.
    # we should remove this when we upgrade to a python 2.6+ compatible version
    # of jython
    #_DEF = object()  This causes the test loader to fail (why?)
    class _dummy(): pass
    _DEF = _dummy

    def next(iter, default=_DEF):
        try:
            return iter.next()
        except StopIteration:
            if default is _DEF:
                raise
            else:
                return default
    # I may goto hell for this:
    import __builtin__
    __builtin__.__dict__['next'] = next


def free_tcp_ports(count=1):
  """ return a list of 'count' TCP ports that are free to used (ie. unbound)
  """
  retry = 0
  ports = []
  sockets = []
  while len(ports) != count:
    port = randint(49152, 65535)
    sockets.append( socket( AF_INET, SOCK_STREAM ) )
    try:
      sockets[-1].bind( ("0.0.0.0", port ) )
      ports.append( port )
      retry = 0
    except:
      retry += 1
      assert retry != 100, "No free sockets available for test!"
  for s in sockets:
    s.close()
  return ports

def free_tcp_port():
  return free_tcp_ports(1)[0]

def pump_uni(src, dst, buffer_size=1024):
  p = src.pending()
  c = dst.capacity()

  if c < 0:
    if p < 0:
      return False
    else:
      src.close_head()
      return True

  if p < 0:
    dst.close_tail()
  elif p == 0 or c == 0:
    return False
  else:
    binary = src.peek(min(c, buffer_size))
    dst.push(binary)
    src.pop(len(binary))

  return True

def pump(transport1, transport2, buffer_size=1024):
  """ Transfer all pending bytes between two Proton engines
      by repeatedly calling peek/pop and push.
      Asserts that each engine accepts some bytes every time
      (unless it's already closed).
  """
  while (pump_uni(transport1, transport2, buffer_size) or
         pump_uni(transport2, transport1, buffer_size)):
    pass

def findfileinpath(filename, searchpath):
    """Find filename in the searchpath
        return absolute path to the file or None
    """
    paths = searchpath.split(os.pathsep)
    for path in paths:
        if os.path.exists(os.path.join(path, filename)):
            return os.path.abspath(os.path.join(path, filename))
    return None

def isSSLPresent():
    return SSL.present()

createdSASLDb = False

def _cyrusSetup(conf_dir):
  """Write out simple SASL config.
  """
  saslpasswd = ""
  if 'SASLPASSWD' in os.environ:
    saslpasswd = os.environ['SASLPASSWD']
  else:
    saslpasswd = findfileinpath('saslpasswd2', os.getenv('PATH')) or ""
  if os.path.exists(saslpasswd):
    t = Template("""sasldb_path: ${db}
mech_list: EXTERNAL DIGEST-MD5 SCRAM-SHA-1 CRAM-MD5 PLAIN ANONYMOUS
""")
    abs_conf_dir = os.path.abspath(conf_dir)
    subprocess.call(args=['rm','-rf',abs_conf_dir])
    os.mkdir(abs_conf_dir)
    db = os.path.join(abs_conf_dir,'proton.sasldb')
    conf = os.path.join(abs_conf_dir,'proton-server.conf')
    f = open(conf, 'w')
    f.write(t.substitute(db=db))
    f.close()

    cmd_template = Template("echo password | ${saslpasswd} -c -p -f ${db} -u proton user")
    cmd = cmd_template.substitute(db=db, saslpasswd=saslpasswd)
    subprocess.call(args=cmd, shell=True)

    os.environ['PN_SASL_CONFIG_PATH'] = abs_conf_dir
    global createdSASLDb
    createdSASLDb = True

# Globally initialize Cyrus SASL configuration
if SASL.extended():
  _cyrusSetup('sasl_conf')

def ensureCanTestExtendedSASL():
  if not SASL.extended():
    raise Skipped('Extended SASL not supported')
  if not createdSASLDb:
    raise Skipped("Can't Test Extended SASL: Couldn't create auth db")

class DefaultConfig:
    defines = {}

class Test(TestCase):
  config = DefaultConfig()

  def __init__(self, name):
    super(Test, self).__init__(name)
    self.name = name

  def configure(self, config):
    self.config = config

  def default(self, name, value, **profiles):
    default = value
    profile = self.config.defines.get("profile")
    if profile:
      default = profiles.get(profile, default)
    return self.config.defines.get(name, default)

  @property
  def delay(self):
    return float(self.default("delay", "1", fast="0.1"))

  @property
  def timeout(self):
    return float(self.default("timeout", "60", fast="10"))

  @property
  def verbose(self):
    return int(self.default("verbose", 0))


class Skipped(SkipTest):
  skipped = True


class TestServer(object):
  """ Base class for creating test-specific message servers.
  """
  def __init__(self, **kwargs):
    self.args = kwargs
    self.reactor = Container(self)
    self.host = "127.0.0.1"
    self.port = 0
    if "host" in kwargs:
      self.host = kwargs["host"]
    if "port" in kwargs:
      self.port = kwargs["port"]
    self.handlers = [CFlowController(10), CHandshaker()]
    self.thread = Thread(name="server-thread", target=self.run)
    self.thread.daemon = True
    self.running = True
    self.conditions = []

  def start(self):
    self.reactor.start()
    retry = 0
    if self.port == 0:
      self.port = str(randint(49152, 65535))
      retry = 10
    while retry > 0:
      try:
        self.acceptor = self.reactor.acceptor(self.host, self.port)
        break
      except IOError:
        self.port = str(randint(49152, 65535))
        retry -= 1
    assert retry > 0, "No free port for server to listen on!"
    self.thread.start()

  def stop(self):
    self.running = False
    self.reactor.wakeup()
    self.thread.join()

  # Note: all following methods all run under the thread:

  def run(self):
    self.reactor.timeout = 3.14159265359
    while self.reactor.process():
      if not self.running:
        self.acceptor.close()
        self.reactor.stop()
        break

  def on_connection_bound(self, event):
    if "idle_timeout" in self.args:
      event.transport.idle_timeout = self.args["idle_timeout"]

  def on_connection_local_close(self, event):
    self.conditions.append(event.connection.condition)

  def on_delivery(self, event):
    event.delivery.settle()

