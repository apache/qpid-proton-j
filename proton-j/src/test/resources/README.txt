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

# The various PKCS12 SSL stores and certificates were created with the following commands:
# Seems to require the JDK8 keytool.

# Clean up existing files
# -----------------------
rm -f *.crt *.csr *.keystore *.truststore

# Create a key and self-signed certificate for the CA, to sign certificate requests and use for trust:
# ----------------------------------------------------------------------------------------------------
keytool -storetype jks -keystore ca-jks.keystore -storepass password -keypass password -alias ca -genkey -keyalg "RSA" -keysize 2048 -dname "O=My Trusted Inc.,CN=my-ca.org" -validity 9999 -ext bc:c=ca:true
keytool -storetype jks -keystore ca-jks.keystore -storepass password -alias ca -exportcert -rfc > ca.crt

# Create a key pair for the server, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype jks -keystore server-jks.keystore -storepass password -keypass password -alias server -genkey -keyalg "RSA" -keysize 2048 -dname "O=Server,CN=localhost" -validity 9999 -ext bc=ca:false -ext eku=sA

keytool -storetype jks -keystore server-jks.keystore -storepass password -alias server -certreq -file server.csr
keytool -storetype jks -keystore ca-jks.keystore -storepass password -alias ca -gencert -rfc -infile server.csr -outfile server.crt -validity 9999 -ext bc=ca:false -ext eku=sA

keytool -storetype jks -keystore server-jks.keystore -storepass password -keypass password -importcert -alias ca -file ca.crt -noprompt
keytool -storetype jks -keystore server-jks.keystore -storepass password -keypass password -importcert -alias server -file server.crt

# Create trust store for the server, import the CA cert:
# -------------------------------------------------------
keytool -storetype jks -keystore server-jks.truststore -storepass password -keypass password -importcert -alias ca -file ca.crt -noprompt

# Create a key pair for a client, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype jks -keystore client-jks.keystore -storepass password -keypass password -alias client -genkey -keyalg "RSA" -keysize 2048 -dname "O=Client,CN=client" -validity 9999 -ext bc=ca:false -ext eku=cA

keytool -storetype jks -keystore client-jks.keystore -storepass password -alias client -certreq -file client.csr
keytool -storetype jks -keystore ca-jks.keystore -storepass password -alias ca -gencert -rfc -infile client.csr -outfile client.crt -validity 9999 -ext bc=ca:false -ext eku=cA

keytool -storetype jks -keystore client-jks.keystore -storepass password -keypass password -importcert -alias ca -file ca.crt -noprompt
keytool -storetype jks -keystore client-jks.keystore -storepass password -keypass password -importcert -alias client -file client.crt

# Create trust store for the client, import the CA cert:
# -------------------------------------------------------
keytool -storetype jks -keystore client-jks.truststore -storepass password -keypass password -importcert -alias ca -file ca.crt -noprompt


# Create a key and self-signed certificate for a second CA, to sign certificate requests and use for trust:
# ---------------------------------------------------------------------------------------------------------

keytool -storetype jks -keystore ca2-jks.keystore -storepass password -keypass password -alias ca2 -genkey -keyalg "RSA" -keysize 2048 -dname "O=My Other Trusted Inc.,CN=my-ca2.org" -validity 9999 -ext bc:c=ca:true
keytool -storetype jks -keystore ca2-jks.keystore -storepass password -alias ca2 -exportcert -rfc > ca2.crt

# Create a key pair for a second server, and sign it with the second CA:
# ----------------------------------------------------------------------
keytool -storetype jks -keystore server2-jks.keystore -storepass password -keypass password -alias server2 -genkey -keyalg "RSA" -keysize 2048 -dname "O=Server2,CN=localhost" -validity 9999 -ext bc=ca:false -ext eku=sA

keytool -storetype jks -keystore server2-jks.keystore -storepass password -alias server2 -certreq -file server2.csr
keytool -storetype jks -keystore ca2-jks.keystore -storepass password -alias ca2 -gencert -rfc -infile server2.csr -outfile server2.crt -validity 9999 -ext bc=ca:false -ext eku=sA

keytool -storetype jks -keystore server2-jks.keystore -storepass password -keypass password -importcert -alias ca2 -file ca2.crt -noprompt
keytool -storetype jks -keystore server2-jks.keystore -storepass password -keypass password -importcert -alias server2 -file server2.crt

# Create a file containing both CA certs to use for trusting both:
# ----------------------------------------------------------------
cat ca.crt > ca-certs.crt
cat ca2.crt >> ca-certs.crt
