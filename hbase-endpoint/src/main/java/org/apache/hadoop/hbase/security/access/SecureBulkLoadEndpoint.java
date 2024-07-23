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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.SecureBulkLoadManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadHFilesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadHFilesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadService;

/**
 * Coprocessor service for bulk loads in secure mode.
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
 */
@CoreCoprocessor
@InterfaceAudience.Private
@Deprecated
public class SecureBulkLoadEndpoint extends SecureBulkLoadService implements RegionCoprocessor {
  public static final long VERSION = 0L;

  private static final Logger LOG = LoggerFactory.getLogger(SecureBulkLoadEndpoint.class);

  private RegionCoprocessorEnvironment env;
  private RegionServerServices rsServices;

  @Override
  public void start(CoprocessorEnvironment env) {
    this.env = (RegionCoprocessorEnvironment) env;
    rsServices = ((HasRegionServerServices) this.env).getRegionServerServices();
    LOG.warn("SecureBulkLoadEndpoint is deprecated. It will be removed in future releases.");
    LOG.warn("Secure bulk load has been integrated into HBase core.");
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public void prepareBulkLoad(RpcController controller, PrepareBulkLoadRequest request,
    RpcCallback<PrepareBulkLoadResponse> done) {
    try {
      SecureBulkLoadManager secureBulkLoadManager = this.rsServices.getSecureBulkLoadManager();

      String bulkToken =
        secureBulkLoadManager.prepareBulkLoad((HRegion) this.env.getRegion(), request);
      done.run(PrepareBulkLoadResponse.newBuilder().setBulkToken(bulkToken).build());
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(null);
  }

  @Override
  public void cleanupBulkLoad(RpcController controller, CleanupBulkLoadRequest request,
    RpcCallback<CleanupBulkLoadResponse> done) {
    try {
      SecureBulkLoadManager secureBulkLoadManager = this.rsServices.getSecureBulkLoadManager();
      secureBulkLoadManager.cleanupBulkLoad((HRegion) this.env.getRegion(), request);
      done.run(CleanupBulkLoadResponse.newBuilder().build());
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(null);
  }

  @Override
  public void secureBulkLoadHFiles(RpcController controller, SecureBulkLoadHFilesRequest request,
    RpcCallback<SecureBulkLoadHFilesResponse> done) {
    boolean loaded = false;
    Map<byte[], List<Path>> map = null;
    try {
      SecureBulkLoadManager secureBulkLoadManager = this.rsServices.getSecureBulkLoadManager();
      BulkLoadHFileRequest bulkLoadHFileRequest = ConvertSecureBulkLoadHFilesRequest(request);
      map = secureBulkLoadManager.secureBulkLoadHFiles((HRegion) this.env.getRegion(),
        bulkLoadHFileRequest);
      loaded = map != null && !map.isEmpty();
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    }
    done.run(SecureBulkLoadHFilesResponse.newBuilder().setLoaded(loaded).build());
  }

  private BulkLoadHFileRequest
    ConvertSecureBulkLoadHFilesRequest(SecureBulkLoadHFilesRequest request) {
    BulkLoadHFileRequest.Builder bulkLoadHFileRequest = BulkLoadHFileRequest.newBuilder();
    RegionSpecifier region = RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME,
      this.env.getRegionInfo().getRegionName());
    bulkLoadHFileRequest.setRegion(region).setFsToken(request.getFsToken())
      .setBulkToken(request.getBulkToken()).setAssignSeqNum(request.getAssignSeqNum())
      .addAllFamilyPath(request.getFamilyPathList());
    return bulkLoadHFileRequest.build();
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }
}
