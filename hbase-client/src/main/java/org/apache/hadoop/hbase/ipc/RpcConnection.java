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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;
import org.apache.hbase.thirdparty.io.netty.util.TimerTask;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

/**
 * Base class for ipc connection.
 */
@InterfaceAudience.Private
abstract class RpcConnection {

  private static final Logger LOG = LoggerFactory.getLogger(RpcConnection.class);

  protected final ConnectionId remoteId;

  protected final boolean useSasl;

  protected final Token<? extends TokenIdentifier> token;

  protected final SecurityInfo securityInfo;

  protected final int reloginMaxBackoff; // max pause before relogin on sasl failure

  protected final Codec codec;

  protected final CompressionCodec compressor;

  protected final MetricsConnection metrics;

  protected final HashedWheelTimer timeoutTimer;

  protected final Configuration conf;

  protected static String CRYPTO_AES_ENABLED_KEY = "hbase.rpc.crypto.encryption.aes.enabled";

  protected static boolean CRYPTO_AES_ENABLED_DEFAULT = false;

  // the last time we were picked up from connection pool.
  protected long lastTouched;

  protected SaslClientAuthenticationProvider provider;

  // Record the server principal which we have successfully authenticated with the remote server
  // this is used to save the extra round trip with server when there are multiple candidate server
  // principals for a given rpc service, like ClientMetaService.
  // See HBASE-28321 for more details.
  private String lastSucceededServerPrincipal;

  protected RpcConnection(Configuration conf, HashedWheelTimer timeoutTimer, ConnectionId remoteId,
    String clusterId, boolean isSecurityEnabled, Codec codec, CompressionCodec compressor,
    MetricsConnection metrics) throws IOException {
    this.timeoutTimer = timeoutTimer;
    this.codec = codec;
    this.compressor = compressor;
    this.conf = conf;
    this.metrics = metrics;
    User ticket = remoteId.getTicket();
    this.securityInfo = SecurityInfo.getInfo(remoteId.getServiceName());
    this.useSasl = isSecurityEnabled;

    // Choose the correct Token and AuthenticationProvider for this client to use
    SaslClientAuthenticationProviders providers =
      SaslClientAuthenticationProviders.getInstance(conf);
    Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> pair;
    if (useSasl && securityInfo != null) {
      pair = providers.selectProvider(clusterId, ticket);
      if (pair == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found no valid authentication method from providers={} with tokens={}",
            providers.toString(), ticket.getTokens());
        }
        throw new RuntimeException("Found no valid authentication method from options");
      }
    } else if (!useSasl) {
      // Hack, while SIMPLE doesn't go via SASL.
      pair = providers.getSimpleProvider();
    } else {
      throw new RuntimeException("Could not compute valid client authentication provider");
    }

    this.provider = pair.getFirst();
    this.token = pair.getSecond();

    LOG.debug("Using {} authentication for service={}, sasl={}",
      provider.getSaslAuthMethod().getName(), remoteId.serviceName, useSasl);
    reloginMaxBackoff = conf.getInt("hbase.security.relogin.maxbackoff", 5000);
    this.remoteId = remoteId;
  }

  protected final void scheduleTimeoutTask(final Call call) {
    if (call.timeout > 0) {
      call.timeoutTask = timeoutTimer.newTimeout(new TimerTask() {

        @Override
        public void run(Timeout timeout) throws Exception {
          call.setTimeout(new CallTimeoutException(call.toShortString() + ", waitTime="
            + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + "ms, rpcTimeout="
            + call.timeout + "ms"));
          callTimeout(call);
        }
      }, call.timeout, TimeUnit.MILLISECONDS);
    }
  }

  // will be overridden in tests
  protected byte[] getConnectionHeaderPreamble() {
    // Assemble the preamble up in a buffer first and then send it. Writing individual elements,
    // they are getting sent across piecemeal according to wireshark and then server is messing
    // up the reading on occasion (the passed in stream is not buffered yet).

    // Preamble is six bytes -- 'HBas' + VERSION + AUTH_CODE
    int rpcHeaderLen = HConstants.RPC_HEADER.length;
    byte[] preamble = new byte[rpcHeaderLen + 2];
    System.arraycopy(HConstants.RPC_HEADER, 0, preamble, 0, rpcHeaderLen);
    preamble[rpcHeaderLen] = HConstants.RPC_CURRENT_VERSION;
    synchronized (this) {
      preamble[rpcHeaderLen + 1] = provider.getSaslAuthMethod().getCode();
    }
    return preamble;
  }

  protected final ConnectionHeader getConnectionHeader() {
    final ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setServiceName(remoteId.getServiceName());
    final UserInformation userInfoPB = provider.getUserInfo(remoteId.ticket);
    if (userInfoPB != null) {
      builder.setUserInfo(userInfoPB);
    }
    if (this.codec != null) {
      builder.setCellBlockCodecClass(this.codec.getClass().getCanonicalName());
    }
    if (this.compressor != null) {
      builder.setCellBlockCompressorClass(this.compressor.getClass().getCanonicalName());
    }
    builder.setVersionInfo(ProtobufUtil.getVersionInfo());
    boolean isCryptoAESEnable = conf.getBoolean(CRYPTO_AES_ENABLED_KEY, CRYPTO_AES_ENABLED_DEFAULT);
    // if Crypto AES enable, setup Cipher transformation
    if (isCryptoAESEnable) {
      builder.setRpcCryptoCipherTransformation(
        conf.get("hbase.rpc.crypto.encryption.aes.cipher.transform", "AES/CTR/NoPadding"));
    }
    return builder.build();
  }

  protected final InetSocketAddress getRemoteInetAddress(MetricsConnection metrics)
    throws UnknownHostException {
    if (metrics != null) {
      metrics.incrNsLookups();
    }
    InetSocketAddress remoteAddr = Address.toSocketAddress(remoteId.getAddress());
    if (remoteAddr.isUnresolved()) {
      if (metrics != null) {
        metrics.incrNsLookupsFailed();
      }
      throw new UnknownHostException(remoteId.getAddress() + " could not be resolved");
    }
    return remoteAddr;
  }

  private static boolean useCanonicalHostname(Configuration conf) {
    return !conf.getBoolean(
      SecurityConstants.UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS,
      SecurityConstants.DEFAULT_UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS);
  }

  private static String getHostnameForServerPrincipal(Configuration conf, InetAddress addr) {
    final String hostname;
    if (useCanonicalHostname(conf)) {
      hostname = addr.getCanonicalHostName();
      if (hostname.equals(addr.getHostAddress())) {
        LOG.warn("Canonical hostname for SASL principal is the same with IP address: " + hostname
          + ", " + addr.getHostName() + ". Check DNS configuration or consider "
          + SecurityConstants.UNSAFE_HBASE_CLIENT_KERBEROS_HOSTNAME_DISABLE_REVERSEDNS + "=true");
      }
    } else {
      hostname = addr.getHostName();
    }

    return hostname.toLowerCase();
  }

  private static String getServerPrincipal(Configuration conf, String serverKey, InetAddress server)
      throws IOException {
      String hostname = getHostnameForServerPrincipal(conf, server);
      return SecurityUtil.getServerPrincipal(conf.get(serverKey), hostname);
  }

  protected final boolean isKerberosAuth() {
    return provider.getSaslAuthMethod().getCode() == AuthMethod.KERBEROS.code;
  }

  protected final Set<String> getServerPrincipals() throws IOException {
    // for authentication method other than kerberos, we do not need to know the server principal
    if (!isKerberosAuth()) {
      return Collections.singleton(HConstants.EMPTY_STRING);
    }
    // if we have successfully authenticated last time, just return the server principal we use last
    // time
    if (lastSucceededServerPrincipal != null) {
      return Collections.singleton(lastSucceededServerPrincipal);
    }
    InetAddress server =
      new InetSocketAddress(remoteId.address.getHostName(), remoteId.address.getPort())
        .getAddress();
    // Even if we have multiple config key in security info, it is still possible that we configured
    // the same principal for them, so here we use a Set
    Set<String> serverPrincipals = new TreeSet<>();
    for (String serverPrincipalKey : securityInfo.getServerPrincipals()) {
      serverPrincipals.add(getServerPrincipal(conf, serverPrincipalKey, server));
    }
    return serverPrincipals;
  }

  protected final void saslNegotiationDone(String serverPrincipal, boolean succeed) {
    LOG.debug("sasl negotiation done with serverPrincipal = {}, succeed = {}", serverPrincipal,
      succeed);
  }

  protected abstract void callTimeout(Call call);

  public ConnectionId remoteId() {
    return remoteId;
  }

  public long getLastTouched() {
    return lastTouched;
  }

  public void setLastTouched(long lastTouched) {
    this.lastTouched = lastTouched;
  }

  /**
   * Tell the idle connection sweeper whether we could be swept.
   */
  public abstract boolean isActive();

  /**
   * Just close connection. Do not need to remove from connection pool.
   */
  public abstract void shutdown();

  public abstract void sendRequest(Call call, HBaseRpcController hrc) throws IOException;

  /**
   * Does the clean up work after the connection is removed from the connection pool
   */
  public abstract void cleanupConnection();
}
