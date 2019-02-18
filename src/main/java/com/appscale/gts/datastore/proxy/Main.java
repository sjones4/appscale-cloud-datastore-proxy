/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static spark.Spark.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.appscale.gts.datastore.proxy.RemoteRpcException.Type;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import spark.Request;
import spark.Response;
import sun.misc.HexDumpEncoder;

/**
 * Entry point for cloud datastore proxy
 */
public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  private static DatastoreService datastoreService = new RemoteDatastoreService();
  private static CloudDatastoreService cloudDatastoreService = new CloudDatastoreProxyService(datastoreService);

  private static String requestInfoToString( Request request) {
    return
        request.requestMethod( ) + " " +
        request.url( ) + "\n" +
        request.headers( ).stream( )
            .reduce("", (headers, header) -> headers + header + ": " + request.headers(header) + "\n") +
            new HexDumpEncoder().encode(request.bodyAsBytes());
  }

  private static String responseHeadersToString(Response response) {
    return
        response.status() + "\n" +
        response.raw().getHeaderNames().stream()
            .reduce("", (headers, header) -> headers + header + ": " + response.raw().getHeader(header) + "\n");
  }

  private static byte[] handleProto(final String projectId, final String method, final byte[] data) {
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

  private static byte[] error(final String message) {
    return error(Code.INTERNAL, message);
  }

  private static byte[] error(final Code code, final String message) {
    return Status.newBuilder()
        .setCode(code.getNumber())
        .setMessage(message)
        .build().toByteArray();
  }

  /**
   * https://cloud.google.com/datastore/docs/reference/data/rest/
   */
  public static void main(final String[] args) {
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
      System.out.println(requestInfoToString(request));
    });
    after((request, response) -> {
      System.out.println(responseHeadersToString(response));
    });
  }
}
