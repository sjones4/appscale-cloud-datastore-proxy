/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static com.appscale.gts.datastore.proxy.DatastoreTypeTranslator.translate;
import java.util.function.Function;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.BeginTransactionResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RollbackResponse;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;

/**
 * Implementation of CloudDatastoreService in terms of DatastoreService
 */
class CloudDatastoreProxyService implements CloudDatastoreService {

  private final DatastoreService datastoreService;

  public CloudDatastoreProxyService(final DatastoreService datastoreService) {
    this.datastoreService = datastoreService;
  };

  private <T> T doWithDatastore(final Function<DatastoreService, T> callback){
    return callback.apply(datastoreService);
  }

  @Override
  public AllocateIdsResponse allocateIds(final AllocateIdsRequest request) {
    return doWithDatastore(datastore -> translate(datastore.allocateIds(translate(request)), request));
  }

  @Override
  public BeginTransactionResponse beginTransaction(final BeginTransactionRequest request) {
    return doWithDatastore(datastore -> translate(datastore.beginTransaction(translate(request))));
  }

  @Override
  public CommitResponse commit(final CommitRequest request) {
    return doWithDatastore(datastore -> translate(datastore.commit(translate(request))));
  }

  @Override
  public LookupResponse lookup(final LookupRequest request) {
    return doWithDatastore(datastore -> translate(datastore.get(translate(request))));
  }

  @Override
  public RollbackResponse rollback(final RollbackRequest request) {
    return doWithDatastore(datastore -> translate(datastore.rollback(translate(request))));
  }

  @Override
  public RunQueryResponse runQuery(final RunQueryRequest request) {
    return doWithDatastore(datastore -> translate(datastore.runQuery(translate(request))));
  }
}
