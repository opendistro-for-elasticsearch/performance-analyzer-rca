package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.sun.net.httpserver.HttpServer;

/** A wrapper class to return all the server created by the App. */
public class ClientServers {
  /** Http server responds to the curl requests. */
  private final HttpServer httpServer;

  /** The net server is the gRPC server. All inter-node communication is gRPC based. */
  private final NetServer netServer;

  /** Client to make gRPC requests. */
  private final NetClient netClient;

  public ClientServers(HttpServer httpServer, NetServer netServer, NetClient netClient) {
    this.httpServer = httpServer;
    this.netServer = netServer;
    this.netClient = netClient;
  }

  public HttpServer getHttpServer() {
    return httpServer;
  }

  public NetServer getNetServer() {
    return netServer;
  }

  public NetClient getNetClient() {
    return netClient;
  }
}
