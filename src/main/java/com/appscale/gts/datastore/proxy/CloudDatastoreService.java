/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

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
 * API for cloud datastore
 */
public interface CloudDatastoreService {

  AllocateIdsResponse allocateIds(AllocateIdsRequest request);

  BeginTransactionResponse beginTransaction(BeginTransactionRequest request);

  CommitResponse commit(CommitRequest request);

  LookupResponse lookup(LookupRequest request);

  RollbackResponse rollback(RollbackRequest request);

  RunQueryResponse runQuery(RunQueryRequest request);
}
