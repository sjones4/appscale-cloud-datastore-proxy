/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import com.google.appengine.api.datastore.DatastoreApiHelper;
import com.google.appengine.repackaged.com.google.common.base.MoreObjects;
import com.google.appengine.repackaged.com.google.common.collect.ImmutableMap;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolMessage;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;
import com.google.apphosting.api.ApiBasePb.VoidProto;
import com.google.apphosting.api.ApiProxy.ApplicationException;
import com.google.apphosting.api.DatastorePb.AllocateIdsRequest;
import com.google.apphosting.api.DatastorePb.AllocateIdsResponse;
import com.google.apphosting.api.DatastorePb.BeginTransactionRequest;
import com.google.apphosting.api.DatastorePb.CommitResponse;
import com.google.apphosting.api.DatastorePb.DatastoreService_3.Method;
import com.google.apphosting.api.DatastorePb.DeleteRequest;
import com.google.apphosting.api.DatastorePb.DeleteResponse;
import com.google.apphosting.api.DatastorePb.GetRequest;
import com.google.apphosting.api.DatastorePb.GetResponse;
import com.google.apphosting.api.DatastorePb.PutRequest;
import com.google.apphosting.api.DatastorePb.PutResponse;
import com.google.apphosting.api.DatastorePb.Query;
import com.google.apphosting.api.DatastorePb.QueryResult;
import com.google.apphosting.api.DatastorePb.Transaction;

/**
 * Implementation of DatastoreService api using Remote RPC to datastore
 */
class RemoteDatastoreService implements DatastoreService {

  private static final ThreadLocal<String> projectId = new ThreadLocal<>();

  private final RemoteRpc remoteRpc;

  private final String host;

  private final int port;

  public RemoteDatastoreService(
      final String host,
      final int port
  ) {
    this.remoteRpc = new RemoteRpc();
    this.host = host;
    this.port = port;
  }

  public RemoteDatastoreService() {
    this("localhost", 4000);
  }

  public static void setProjectIdContext(final String projectId) {
    RemoteDatastoreService.projectId.set(projectId);
  }

  public <T extends ProtocolMessage<T>> T call(
      final Method method,
      final ProtocolMessage<?> request,
      final T responseProto
  ) {
    try {
      final byte[] responseBytes = remoteRpc.call(
          this.host,
          this.port,
          "datastore_v3",
          method.name(),
          ImmutableMap.of(
              "AppData", MoreObjects.firstNonNull(projectId.get(), ""),
              "Module", "default",
              "Version", "v1"),
          request.toByteArray());
      if (responseBytes != null) {
        if (!responseProto.parseFrom(responseBytes)) {
          throw new InvalidProtocolBufferException(
              String.format("Invalid %s.%s response", "datastore_v3", method.name()));
        }
        String initializationError = responseProto.findInitializationError();
        if (initializationError != null) {
          throw new InvalidProtocolBufferException(initializationError);
        }
      }
      return responseProto;
    } catch (final Exception e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof ApplicationException) { //TODO remove if not useful
          throw DatastoreApiHelper.translateError((ApplicationException) cause);
        }
        cause = cause.getCause();
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public AllocateIdsResponse allocateIds(final AllocateIdsRequest request) {
    return call(Method.AllocateIds, request, new AllocateIdsResponse());
  }

  @Override
  public Transaction beginTransaction(final BeginTransactionRequest request) {
    return call(Method.BeginTransaction, request, new Transaction());
  }

  @Override
  public CommitResponse commit(final Transaction request) {
    return call(Method.Commit, request, new CommitResponse());
  }

  @Override
  public VoidProto rollback(final Transaction request) {
    return call(Method.Rollback, request, new VoidProto());
  }

  @Override
  public GetResponse get(final GetRequest request) {
    return call(Method.Get, request, new GetResponse());
  }

  @Override
  public PutResponse put(final PutRequest request) {
    return call(Method.Put, request, new PutResponse());
  }

  @Override
  public DeleteResponse delete(final DeleteRequest request) {
    return call(Method.Delete, request, new DeleteResponse());
  }

  @Override
  public QueryResult runQuery(final Query request) {
    return call(Method.RunQuery, request, new QueryResult());
  }
}
