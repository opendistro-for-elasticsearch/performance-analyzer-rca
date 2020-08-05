/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.net.InetAddresses;

public class InstanceDetails {
  public static class Ip {

    // The only way to get the ip is to get the serialized string representation of it.
    private String ip;

    public Ip(String ip) {
      if (!InetAddresses.isInetAddress(ip)) {
        throw new IllegalArgumentException("The provided string is not an IPV4ip: '" + ip + "'");
      }
      this.ip = ip;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Ip)) {
        return false;
      }
      Ip ip1 = (Ip) o;
      return Objects.equal(ip, ip1.ip);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(ip);
    }

    @Override
    public String toString() {
      return ip;
    }
  }

  public static class Id {
    private String id;

    public Id(String id) {
      if (InetAddresses.isInetAddress(id)) {
        throw new IllegalArgumentException("The provided string is in the form an IPV4 address: '" + id
                + "'. Are you sure this is the host ID");
      }
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Id)) {
        return false;
      }
      Id id1 = (Id) o;
      return Objects.equal(id, id1.id);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id);
    }

    @Override
    public String toString() {
      return id;
    }
  }


  private final AllMetrics.NodeRole role;
  private final Id instanceId;
  private final Ip instanceIp;
  private final boolean isMaster;
  private final int grpcPort;

  public InstanceDetails(AllMetrics.NodeRole role, Id instanceId, Ip instanceIp, boolean isMaster) {
    this(role, instanceId, instanceIp, isMaster, Util.RPC_PORT);
  }

  public InstanceDetails(AllMetrics.NodeRole role, Id instanceId, Ip instanceIp, boolean isMaster, int grpcPort) {
    this.role = role;
    this.instanceId = instanceId;
    this.instanceIp = instanceIp;
    this.isMaster = isMaster;
    this.grpcPort = grpcPort;
  }

  public InstanceDetails(ClusterDetailsEventProcessor.NodeDetails nodeDetails) {
    this(AllMetrics.NodeRole.valueOf(nodeDetails.getRole()),
            new Id(nodeDetails.getId()),
            new Ip(nodeDetails.getHostAddress()),
            nodeDetails.getIsMasterNode(),
            nodeDetails.getGrpcPort());
  }

  public InstanceDetails(AllMetrics.NodeRole role) {
    this(role, new Id("unknown"), new Ip("0.0.0.0"), false);
  }

  @VisibleForTesting
  public InstanceDetails(Id instanceId, Ip instanceIp, int myGrpcServerPort) {
    this(AllMetrics.NodeRole.UNKNOWN, instanceId, instanceIp, false, myGrpcServerPort);
  }

  public AllMetrics.NodeRole getRole() {
    return isMaster ? NodeRole.ELECTED_MASTER : role;
  }

  public Id getInstanceId() {
    return instanceId;
  }

  public Ip getInstanceIp() {
    return instanceIp;
  }

  public boolean getIsMaster() {
    return isMaster;
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InstanceDetails)) {
      return false;
    }
    InstanceDetails that = (InstanceDetails) o;
    return isMaster == that.isMaster
            && getGrpcPort() == that.getGrpcPort()
            && getRole() == that.getRole()
            && Objects.equal(getInstanceId(), that.getInstanceId())
            && Objects.equal(getInstanceIp(), that.getInstanceIp());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getRole(), getInstanceId(), getInstanceIp(), isMaster, getGrpcPort());
  }

  @Override
  public String toString() {
    return ""
            + instanceId + "::"
            + instanceIp + "::"
            + role + "::"
            + grpcPort;
  }
}
