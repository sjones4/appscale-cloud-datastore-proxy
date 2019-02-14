/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import com.google.apphosting.api.ApiBasePb.VoidProto;
import com.google.apphosting.api.DatastorePb.AllocateIdsRequest;
import com.google.apphosting.api.DatastorePb.AllocateIdsResponse;
import com.google.apphosting.api.DatastorePb.BeginTransactionRequest;
import com.google.apphosting.api.DatastorePb.CommitResponse;
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
 * API for datastore
 */
public interface DatastoreService {
  AllocateIdsResponse allocateIds(AllocateIdsRequest request);

  Transaction beginTransaction(BeginTransactionRequest request);

  CommitResponse commit(Transaction request);

  VoidProto rollback(Transaction request);

  GetResponse get(GetRequest request);

  PutResponse put(PutRequest request);

  DeleteResponse delete(DeleteRequest request);

  QueryResult runQuery(Query request);
}
