/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static spark.Spark.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Set;
import java.util.logging.LogManager;
import com.appscale.gts.datastore.proxy.RemoteRpcException.Type;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import sun.misc.HexDumpEncoder;

/**
 * Entry point for cloud datastore proxy
 */
public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static final Logger traceLog = LoggerFactory.getLogger("com.appscale.gts.datastore.proxy.trace");

  private Config config;
  private DatastoreService datastoreService;
  private CloudDatastoreService cloudDatastoreService;

  private String requestInfoToString( Request request) {
    return
        request.requestMethod( ) + " " +
        request.url( ) + "\n" +
        request.headers( ).stream( )
            .reduce("", (headers, header) -> headers + header + ": " + request.headers(header) + "\n") +
            new HexDumpEncoder().encode(request.bodyAsBytes());
  }

  private String responseHeadersToString(Response response) {
    return
        response.status() + "\n" +
        response.raw().getHeaderNames().stream()
            .reduce("", (headers, header) -> headers + header + ": " + response.raw().getHeader(header) + "\n");
  }

  private byte[] handleProto(final String projectId, final String method, final byte[] data) {
    try {
      RemoteDatastoreService.setProjectIdContext(projectId);
      switch (method) {
        case "allocateIds":
          return cloudDatastoreService.allocateIds(AllocateIdsRequest.parseFrom(data)).toByteArray();
        case "beginTransaction":
          return cloudDatastoreService.beginTransaction(BeginTransactionRequest.parseFrom(data)).toByteArray();
        case "commit":
          return cloudDatastoreService.commit(CommitRequest.parseFrom(data)).toByteArray();
        case "lookup":
          return cloudDatastoreService.lookup(LookupRequest.parseFrom(data)).toByteArray();
        case "rollback":
          return cloudDatastoreService.rollback(RollbackRequest.parseFrom(data)).toByteArray();
        case "runQuery":
          return cloudDatastoreService.runQuery(RunQueryRequest.parseFrom(data)).toByteArray();
        default:
          return error(Code.INVALID_ARGUMENT, "Invalid method: " + method);
      }
    } catch (InvalidProtocolBufferException e) {
      log.warn("Protocol buffer error handling request : " + e.getMessage(), e);
      return error("Unable to handle message");
    } catch (RemoteRpcException e) {
      log.warn("Rpc error handling request : " + e.getMessage(), e);
      if (e.getType().equals(Type.Application)) {
        return error(e.getMessage());
      } else {
        return error("Unable to handle message");
      }
    }
  }

  private byte[] error(final String message) {
    return error(Code.INTERNAL, message);
  }

  private byte[] error(final Code code, final String message) {
    return Status.newBuilder()
        .setCode(code.getNumber())
        .setMessage(message)
        .build().toByteArray();
  }

  private void initLog(final String[] args) {
    final Set<String> argSet = Sets.newHashSet(Arrays.asList(args));
    final Set<String> logConfigResourceNames = Sets.newLinkedHashSet();
    logConfigResourceNames.add("logging-base.properties");
    if (argSet.contains("--console")) {
      logConfigResourceNames.add("logging-console.properties");
    } else {
      logConfigResourceNames.add("logging-file.properties");
    }
    if (argSet.contains("--debug")) {
      logConfigResourceNames.add("logging-debug.properties");
    }
    if (argSet.contains("--trace")) {
      logConfigResourceNames.add("logging-trace.properties");
    }

    try (final InputStream configStream = ByteSource.concat(logConfigResourceNames.stream()
        .map(name -> Resources.asByteSource(Main.class.getResource(name)))
        .iterator()).openStream()) {
      LogManager.getLogManager().readConfiguration(configStream);
    } catch (final IOException e) {
      System.err.println("Logging configuration error");
      e.printStackTrace();
    }
    log.info("Logging configured");
  }

  /**
   * Optional configuration for listener and datastore backend
   */
  private void initConfig(final String[] args) {
    String datastoreHost = "localhost";
    int datastorePort = 8888;
    String listenAddress = "127.0.0.1";
    int listenPort = 3500;

    for (final String arg : args) {
      if (arg.startsWith("--datastore-host=")) {
        datastoreHost = arg.substring(17);
      } else if (arg.startsWith("--datastore-port=")) {
        datastorePort = Integer.parseInt(arg.substring(17));
      } else if (arg.startsWith("--listen-address=")) {
        listenAddress = arg.substring(17);
      } else if (arg.startsWith("--listen-port=")) {
        listenPort = Integer.parseInt(arg.substring(14));
      }
    }

    this.config = new Config(datastoreHost, datastorePort, listenAddress, listenPort);
    log.info("Initialized configuration " + this.config);
  }

  private void initComponents() {
    this.datastoreService = new RemoteDatastoreService(this.config.getDatastoreHost(), this.config.getDatastorePort());
    this.cloudDatastoreService = new CloudDatastoreProxyService(datastoreService);
  }

  private void initService() {
    ipAddress(this.config.getListenAddress());
    port(this.config.getListenPort());
    post("/v1/projects/:project", (req, res) -> {
      // Content-Type: application/x-protobuf
      // x-goog-api-format-version: 2
      final String projectAndMethod = req.params( ":project" );
      final String[] projectAndMethodPair = projectAndMethod.split( ":", 2 );
      if ( projectAndMethodPair.length!=2 ) {
        halt(400, "Invalid path: " + req.url());
      }
      final String project = projectAndMethodPair[0];
      final String method = projectAndMethodPair[1];
      res.header( "Content-Type", "application/x-protobuf" );
      return handleProto(project, method, req.bodyAsBytes());
    });
    before((request, response) -> {
      if (traceLog.isInfoEnabled()) traceLog.info(requestInfoToString(request));
    });
    after((request, response) -> {
      if (traceLog.isInfoEnabled()) traceLog.info(responseHeadersToString(response));
    });
  }

  /**
   * https://cloud.google.com/datastore/docs/reference/data/rest/
   */
  public static void main(final String[] args) {
    final Main main = new Main();
    main.initLog(args);
    main.initConfig(args);
    main.initComponents();
    main.initService();
  }

  private static final class Config {
    private final String datastoreHost;
    private final int datastorePort;
    private final String listenAddress;
    private final int listenPort;

    private Config(
        final String datastoreHost,
        final int datastorePort,
        final String listenAddress,
        final int listenPort
    ) {
      this.datastoreHost = datastoreHost;
      this.datastorePort = datastorePort;
      this.listenAddress = listenAddress;
      this.listenPort = listenPort;
    }

    public String getDatastoreHost() {
      return datastoreHost;
    }

    public int getDatastorePort() {
      return datastorePort;
    }

    public String getListenAddress() {
      return listenAddress;
    }

    public int getListenPort() {
      return listenPort;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("datastoreHost", datastoreHost)
          .add("datastorePort", datastorePort)
          .add("listenAddress", listenAddress)
          .add("listenPort", listenPort)
          .toString();
    }
  }
}
