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
package org.apache.hadoop.hbase.security.visibility;

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.VisibilityLabelsProtos;

/**
 * Utility client for doing visibility labels admin operations.
 */
@InterfaceAudience.Public
public class VisibilityClient {

  /**
   * Return true if cell visibility features are supported and enabled
   * @param connection The connection to use
   * @return true if cell visibility features are supported and enabled, false otherwise
   */
  public static boolean isCellVisibilityEnabled(Connection connection) throws IOException {
    return connection.getAdmin().getSecurityCapabilities()
      .contains(SecurityCapability.CELL_VISIBILITY);
  }

  /**
   * Utility method for adding label to the system.
   * @deprecated Use {@link #addLabel(Connection,String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsProtos.VisibilityLabelsResponse addLabel(Configuration conf,
    final String label) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return addLabels(connection, new String[] { label });
    }
  }

  /**
   * Utility method for adding label to the system.
   */
  public static VisibilityLabelsProtos.VisibilityLabelsResponse addLabel(Connection connection,
    final String label) throws Throwable {
    return addLabels(connection, new String[] { label });
  }

  /**
   * Utility method for adding labels to the system.
   * @deprecated Use {@link #addLabels(Connection,String[])} instead.
   */
  @Deprecated
  public static VisibilityLabelsProtos.VisibilityLabelsResponse addLabels(Configuration conf,
    final String[] labels) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return addLabels(connection, labels);
    }
  }

  /**
   * Utility method for adding labels to the system.
   */
  public static VisibilityLabelsProtos.VisibilityLabelsResponse addLabels(Connection connection,
    final String[] labels) throws Throwable {
    try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
      Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
        VisibilityLabelsProtos.VisibilityLabelsResponse> callable =
          new Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
            VisibilityLabelsProtos.VisibilityLabelsResponse>() {
            ServerRpcController controller = new ServerRpcController();
            CoprocessorRpcUtils.BlockingRpcCallback<
              VisibilityLabelsProtos.VisibilityLabelsResponse> rpcCallback =
                new CoprocessorRpcUtils.BlockingRpcCallback<>();

            @Override
            public VisibilityLabelsProtos.VisibilityLabelsResponse
              call(VisibilityLabelsProtos.VisibilityLabelsService service) throws IOException {
              VisibilityLabelsProtos.VisibilityLabelsRequest.Builder builder =
                VisibilityLabelsProtos.VisibilityLabelsRequest.newBuilder();
              for (String label : labels) {
                if (label.length() > 0) {
                  VisibilityLabelsProtos.VisibilityLabel.Builder newBuilder =
                    VisibilityLabelsProtos.VisibilityLabel.newBuilder();
                  newBuilder.setLabel(UnsafeByteOperations.unsafeWrap(Bytes.toBytes(label)));
                  builder.addVisLabel(newBuilder.build());
                }
              }
              service.addLabels(controller, builder.build(), rpcCallback);
              VisibilityLabelsProtos.VisibilityLabelsResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response;
            }
          };
      Map<byte[], VisibilityLabelsProtos.VisibilityLabelsResponse> result =
        table.coprocessorService(VisibilityLabelsProtos.VisibilityLabelsService.class,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one region for labels
      // table and so one entry in result Map.
    }
  }

  /**
   * Sets given labels globally authorized for the user.
   * @deprecated Use {@link #setAuths(Connection,String[],String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsProtos.VisibilityLabelsResponse setAuths(Configuration conf,
    final String[] auths, final String user) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return setOrClearAuths(connection, auths, user, true);
    }
  }

  /**
   * Sets given labels globally authorized for the user.
   */
  public static VisibilityLabelsProtos.VisibilityLabelsResponse setAuths(Connection connection,
    final String[] auths, final String user) throws Throwable {
    return setOrClearAuths(connection, auths, user, true);
  }

  /**
   * Returns labels, the given user is globally authorized for.
   * @deprecated Use {@link #getAuths(Connection,String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsProtos.GetAuthsResponse getAuths(Configuration conf,
    final String user) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return getAuths(connection, user);
    }
  }

  /**
   * Get the authorization for a given user
   * @param connection the Connection instance to use
   * @param user       the user
   * @return labels the given user is globally authorized for
   */
  public static VisibilityLabelsProtos.GetAuthsResponse getAuths(Connection connection,
    final String user) throws Throwable {
    try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
      Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
        VisibilityLabelsProtos.GetAuthsResponse> callable =
          new Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
            VisibilityLabelsProtos.GetAuthsResponse>() {
            ServerRpcController controller = new ServerRpcController();
            CoprocessorRpcUtils.BlockingRpcCallback<
              VisibilityLabelsProtos.GetAuthsResponse> rpcCallback =
                new CoprocessorRpcUtils.BlockingRpcCallback<>();

            @Override
            public VisibilityLabelsProtos.GetAuthsResponse
              call(VisibilityLabelsProtos.VisibilityLabelsService service) throws IOException {
              VisibilityLabelsProtos.GetAuthsRequest.Builder getAuthReqBuilder =
                VisibilityLabelsProtos.GetAuthsRequest.newBuilder();
              getAuthReqBuilder.setUser(UnsafeByteOperations.unsafeWrap(Bytes.toBytes(user)));
              service.getAuths(controller, getAuthReqBuilder.build(), rpcCallback);
              VisibilityLabelsProtos.GetAuthsResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response;
            }
          };
      Map<byte[], VisibilityLabelsProtos.GetAuthsResponse> result =
        table.coprocessorService(VisibilityLabelsProtos.VisibilityLabelsService.class,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one region for labels
      // table and so one entry in result Map.
    }
  }

  /**
   * Retrieve the list of visibility labels defined in the system.
   * @param conf  the configuration to use
   * @param regex The regular expression to filter which labels are returned.
   * @return labels The list of visibility labels defined in the system.
   * @deprecated Use {@link #listLabels(Connection,String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsProtos.ListLabelsResponse listLabels(Configuration conf,
    final String regex) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return listLabels(connection, regex);
    }
  }

  /**
   * Retrieve the list of visibility labels defined in the system.
   * @param connection The Connection instance to use.
   * @param regex      The regular expression to filter which labels are returned.
   * @return labels The list of visibility labels defined in the system.
   */
  public static VisibilityLabelsProtos.ListLabelsResponse listLabels(Connection connection,
    final String regex) throws Throwable {
    try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
      Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
        VisibilityLabelsProtos.ListLabelsResponse> callable =
          new Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
            VisibilityLabelsProtos.ListLabelsResponse>() {
            ServerRpcController controller = new ServerRpcController();
            CoprocessorRpcUtils.BlockingRpcCallback<
              VisibilityLabelsProtos.ListLabelsResponse> rpcCallback =
                new CoprocessorRpcUtils.BlockingRpcCallback<>();

            @Override
            public VisibilityLabelsProtos.ListLabelsResponse
              call(VisibilityLabelsProtos.VisibilityLabelsService service) throws IOException {
              VisibilityLabelsProtos.ListLabelsRequest.Builder listAuthLabelsReqBuilder =
                VisibilityLabelsProtos.ListLabelsRequest.newBuilder();
              if (regex != null) {
                // Compile the regex here to catch any regex exception earlier.
                Pattern pattern = Pattern.compile(regex);
                listAuthLabelsReqBuilder.setRegex(pattern.toString());
              }
              service.listLabels(controller, listAuthLabelsReqBuilder.build(), rpcCallback);
              VisibilityLabelsProtos.ListLabelsResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response;
            }
          };
      Map<byte[], VisibilityLabelsProtos.ListLabelsResponse> result =
        table.coprocessorService(VisibilityLabelsProtos.VisibilityLabelsService.class,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one region for labels
      // table and so one entry in result Map.
    }
  }

  /**
   * Removes given labels from user's globally authorized list of labels.
   * @deprecated Use {@link #clearAuths(Connection,String[],String)} instead.
   */
  @Deprecated
  public static VisibilityLabelsProtos.VisibilityLabelsResponse clearAuths(Configuration conf,
    final String[] auths, final String user) throws Throwable {
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      return setOrClearAuths(connection, auths, user, false);
    }
  }

  /**
   * Removes given labels from user's globally authorized list of labels.
   */
  public static VisibilityLabelsProtos.VisibilityLabelsResponse clearAuths(Connection connection,
    final String[] auths, final String user) throws Throwable {
    return setOrClearAuths(connection, auths, user, false);
  }

  private static VisibilityLabelsProtos.VisibilityLabelsResponse setOrClearAuths(
    Connection connection, final String[] auths, final String user, final boolean setOrClear)
    throws IOException, ServiceException, Throwable {

    try (Table table = connection.getTable(LABELS_TABLE_NAME)) {
      Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
        VisibilityLabelsProtos.VisibilityLabelsResponse> callable =
          new Batch.Call<VisibilityLabelsProtos.VisibilityLabelsService,
            VisibilityLabelsProtos.VisibilityLabelsResponse>() {
            ServerRpcController controller = new ServerRpcController();
            CoprocessorRpcUtils.BlockingRpcCallback<
              VisibilityLabelsProtos.VisibilityLabelsResponse> rpcCallback =
                new CoprocessorRpcUtils.BlockingRpcCallback<>();

            @Override
            public VisibilityLabelsProtos.VisibilityLabelsResponse
              call(VisibilityLabelsProtos.VisibilityLabelsService service) throws IOException {
              VisibilityLabelsProtos.SetAuthsRequest.Builder setAuthReqBuilder =
                VisibilityLabelsProtos.SetAuthsRequest.newBuilder();
              setAuthReqBuilder.setUser(UnsafeByteOperations.unsafeWrap(Bytes.toBytes(user)));
              for (String auth : auths) {
                if (auth.length() > 0) {
                  setAuthReqBuilder.addAuth(ByteString.copyFromUtf8(auth));
                }
              }
              if (setOrClear) {
                service.setAuths(controller, setAuthReqBuilder.build(), rpcCallback);
              } else {
                service.clearAuths(controller, setAuthReqBuilder.build(), rpcCallback);
              }
              VisibilityLabelsProtos.VisibilityLabelsResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response;
            }
          };
      Map<byte[], VisibilityLabelsProtos.VisibilityLabelsResponse> result =
        table.coprocessorService(VisibilityLabelsProtos.VisibilityLabelsService.class,
          HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, callable);
      return result.values().iterator().next(); // There will be exactly one region for labels
      // table and so one entry in result Map.
    }
  }
}
