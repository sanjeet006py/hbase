/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security;

import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.loginKerberosPrincipal;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.setSecuredConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Security;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContextProvider;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.NettyServerRpcConnection;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

@Category({ SecurityTests.class, MediumTests.class })
public class TestSaslTlsIPCSufficiency {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSaslTlsIPCSufficiency.class);

  private static HBaseCommonTestingUtility UTIL;
  private static File KEYTAB_FILE;
  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;
  private static UserGroupInformation UGI;
  private static File DIR;
  private static X509TestContextProvider PROVIDER;

  private X509TestContext x509TestContext;
  protected RpcServer rpcServer;
  protected RpcClient rpcClient;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HBaseTestingUtility util = new HBaseTestingUtility();
    UTIL = util;
    // Set up TLS
    Security.addProvider(new BouncyCastleProvider());
    DIR = new File(UTIL.getDataTestDir(TestSaslTlsIPCSufficiency.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(DIR);
    Configuration conf = UTIL.getConfiguration();
    conf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, NettyRpcClient.class,
      RpcClient.class);
    conf.setClass(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class,
      RpcServer.class);
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, true);
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, false);
    conf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, true);
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUFFICIENT, true);
    PROVIDER = new X509TestContextProvider(conf, DIR);
    // Set up Kerberos
    KEYTAB_FILE = new File(util.getDataTestDir("keytab").toUri().getPath());
    KDC = util.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    UGI = loginKerberosPrincipal(KEYTAB_FILE.getCanonicalPath(), PRINCIPAL);
    setSecuredConfiguration(util.getConfiguration());
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipals())
      .thenReturn(Collections.singletonList(HBaseKerberosUtils.KRB_PRINCIPAL));
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    // Clean up kerberos
    if (KDC != null) {
      KDC.stop();
    }
    // Clean up TLS
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws Exception {
    x509TestContext = PROVIDER.get(X509KeyType.EC, X509KeyType.EC, "pa$$w0rd".toCharArray());
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    Configuration conf = UTIL.getConfiguration();
    rpcServer = new NettyRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), conf, new FifoRpcScheduler(conf, 1), true) {
      @Override
      protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
        return new NettyServerRpcConnection(this, channel) {
          @Override
          protected void processRequest(ByteBuff buf) throws IOException, InterruptedException {
            // What should happen when TLS is sufficient and strong authentication is also
            // enabled is a forced downgrade to SIMPLE. If SASL negotiation completes after
            // the TLS handshake and useSasl is true we have failed to implement the short cut.
            if (this.useSasl) {
              throw new IOException(
                "SASL negotiation completed instead of falling back to SIMPLE auth");
            }
            super.processRequest(buf);
          }
        };
      }
    };
    rpcServer.start();
    rpcClient = new NettyRpcClient(conf);
  }

  @After
  public void tearDown() throws IOException {
    if (rpcServer != null) {
      rpcServer.stop();
    }
    Closeables.close(rpcClient, true);
    x509TestContext.clearConfigurations();
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }

  @Test
  public void testTlsSufficency() throws Exception {
    BlockingInterface stub = TestProtobufRpcServiceImpl.newBlockingStub(rpcClient,
      rpcServer.getListenerAddress(), User.create(UGI));
    stub.echo(null, EchoRequestProto.newBuilder().setMessage("hello world").build()).getMessage();
  }

}
