/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static spark.Spark.*;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RunQueryRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import spark.Request;

/**
 * Entry point for cloud datastore proxy
 */
public class Main {
  private static DatastoreService datastoreService = new RemoteDatastoreService();
  private static CloudDatastoreService cloudDatastoreService = new CloudDatastoreProxyService(datastoreService);

  private static String requestInfoToString( Request request) {
    return
        request.requestMethod( ) + " " +
        request.url( ) + "\n" +
        request.headers( ).stream( )
            .reduce("", (headers, header) -> headers + header + ": " + request.headers(header) + "\n") +
        request.body( );
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
      }
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    return new byte[0];
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
  }
}
