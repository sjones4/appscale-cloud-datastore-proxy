/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static com.appscale.gts.datastore.proxy.DatastoreTypeTranslator.translate;
import java.util.List;
import java.util.function.Function;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolMessage;
import com.google.apphosting.api.DatastorePb.DeleteRequest;
import com.google.apphosting.api.DatastorePb.PutRequest;
import com.google.apphosting.api.DatastorePb.Transaction;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.BeginTransactionResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.MutationResult;
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
    return doWithDatastore(datastore -> {
      final List<ProtocolMessage<?>> mutations = translate(request);
      final List<MutationResult> mutationResults = Lists.newArrayList();
      Transaction tx = null;
      for (final ProtocolMessage<?> mutation : mutations) {
        if (mutation instanceof DeleteRequest) {
          mutationResults.addAll(translate(datastore.delete((DeleteRequest) mutation)));
        } else if (mutation instanceof PutRequest) {
          mutationResults.addAll(translate(datastore.put((PutRequest) mutation)));
        } else if (mutation instanceof Transaction) {
          tx = (Transaction) mutation;
        } else {
          throw new RuntimeException("Unsupported mutation " + mutation);
        }
      }
      return translate(mutationResults, tx==null ? null : datastore.commit(tx));
    });
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
