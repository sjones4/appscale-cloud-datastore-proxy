/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NO_MORE_RESULTS;
import java.util.List;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.repackaged.com.google.common.primitives.Longs;
import com.google.appengine.repackaged.com.google.protobuf.InvalidProtocolBufferException;
import com.google.apphosting.api.ApiBasePb.VoidProto;
import com.google.apphosting.api.DatastorePb;
import com.google.apphosting.api.DatastorePb.AllocateIdsRequest;
import com.google.apphosting.api.DatastorePb.AllocateIdsResponse;
import com.google.apphosting.api.DatastorePb.BeginTransactionRequest;
import com.google.apphosting.api.DatastorePb.CommitResponse;
import com.google.apphosting.api.DatastorePb.CompiledCursor;
import com.google.apphosting.api.DatastorePb.CompositeIndices;
import com.google.apphosting.api.DatastorePb.Cursor;
import com.google.apphosting.api.DatastorePb.DeleteRequest;
import com.google.apphosting.api.DatastorePb.DeleteResponse;
import com.google.apphosting.api.DatastorePb.GetRequest;
import com.google.apphosting.api.DatastorePb.GetResponse;
import com.google.apphosting.api.DatastorePb.GetResponse.Entity;
import com.google.apphosting.api.DatastorePb.NextRequest;
import com.google.apphosting.api.DatastorePb.PutRequest;
import com.google.apphosting.api.DatastorePb.PutResponse;
import com.google.apphosting.api.DatastorePb.Query;
import com.google.apphosting.api.DatastorePb.Query.Filter;
import com.google.apphosting.api.DatastorePb.Query.Order;
import com.google.apphosting.api.DatastorePb.QueryResult;
import com.google.apphosting.api.DatastorePb.Transaction;
import com.google.datastore.v1.EntityResult.ResultType;
import com.google.datastore.v1.ReadOptions.ReadConsistency;
import com.google.protobuf.ByteString;
import com.google.storage.onestore.v3.OnestoreEntity;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Path;
import com.google.storage.onestore.v3.OnestoreEntity.Path.Element;
import com.google.storage.onestore.v3.OnestoreEntity.Property;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue.ReferenceValue;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue.ReferenceValuePathElement;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;

/**
 * Type translation for datastore vs cloud datastore
 */
class DatastoreTypeTranslator {

  public static AllocateIdsRequest translate(final com.google.datastore.v1.AllocateIdsRequest request) {
    final AllocateIdsRequest allocateIdsRequest = new DatastorePb.AllocateIdsRequest();
    for(final com.google.datastore.v1.Key key : request.getKeysList()) {
      allocateIdsRequest.setModelKey(translate(key));
      break;
    }
    return allocateIdsRequest.freeze();
  }

  public static Reference translate(final com.google.datastore.v1.Key key) {
    final Reference reference = new OnestoreEntity.Reference();
    reference.setApp(key.getPartitionId().getProjectId()); //TODO appid <-> project id conversions (appidentityservice?)
    reference.setNameSpace(key.getPartitionId().getNamespaceId());
    reference.setDatabaseId(key.getPartitionId().getProjectId());
    final Path path = new OnestoreEntity.Path();
    for (final com.google.datastore.v1.Key.PathElement pathElement : key.getPathList()) {
      path.addElement(translate(pathElement));
    }
    reference.setPath(path.freeze());
    return reference.freeze();
  }

  public static com.google.datastore.v1.Key translate(final Reference reference){
    final com.google.datastore.v1.Key.Builder keyBuilder = com.google.datastore.v1.Key.newBuilder();
    final com.google.datastore.v1.PartitionId.Builder partitionBuilder =
        com.google.datastore.v1.PartitionId.newBuilder();
    partitionBuilder.setProjectId(reference.getDatabaseId());
    partitionBuilder.setNamespaceId(reference.getNameSpace());
    keyBuilder.setPartitionId(partitionBuilder.build());
    for (final Element element : reference.getPath().elements()) {
      keyBuilder.addPath(translate(element));
    }
    return keyBuilder.build();
  }

  public static ReferenceValue translateVal(final com.google.datastore.v1.Key key) {
    final ReferenceValue reference = new ReferenceValue();
    reference.setApp(key.getPartitionId().getProjectId());
    reference.setNameSpace(key.getPartitionId().getNamespaceId());
    reference.setDatabaseId(key.getPartitionId().getProjectId());
    for (final com.google.datastore.v1.Key.PathElement pathElement : key.getPathList()) {
      reference.addPathElement(translateElement(pathElement));
    }
    return reference.freeze();
  }

  public static com.google.datastore.v1.Key translate(final ReferenceValue reference){
    final com.google.datastore.v1.Key.Builder keyBuilder = com.google.datastore.v1.Key.newBuilder();
    final com.google.datastore.v1.PartitionId.Builder partitionBuilder =
        com.google.datastore.v1.PartitionId.newBuilder();
    partitionBuilder.setProjectId(reference.getDatabaseId());
    partitionBuilder.setNamespaceId(reference.getNameSpace());
    keyBuilder.setPartitionId(partitionBuilder.build());
    for (final ReferenceValuePathElement element : reference.pathElements()) {
      keyBuilder.addPath(translate(element));
    }
    return keyBuilder.build();
  }

  public static Element translate(final com.google.datastore.v1.Key.PathElement pathElement) {
    final Element element = new OnestoreEntity.Path.Element();
    element.setId(pathElement.getId());
    element.setName(pathElement.getName());
    element.setType(pathElement.getKind());
    return element.freeze();
  }

  public static com.google.datastore.v1.Key.PathElement translate(final Element element) {
    final com.google.datastore.v1.Key.PathElement.Builder pathElement =
        com.google.datastore.v1.Key.PathElement.newBuilder();
    pathElement.setId(element.getId());
    pathElement.setName(element.getName());
    pathElement.setKind(element.getType());
    return pathElement.build();
  }

  public static ReferenceValuePathElement translateElement(final com.google.datastore.v1.Key.PathElement pathElement) {
    final ReferenceValuePathElement element = new ReferenceValuePathElement();
    element.setId(pathElement.getId());
    element.setName(pathElement.getName());
    element.setType(pathElement.getKind());
    return element.freeze();
  }

  public static com.google.datastore.v1.Key.PathElement translate(final ReferenceValuePathElement element) {
    final com.google.datastore.v1.Key.PathElement.Builder pathElement =
        com.google.datastore.v1.Key.PathElement.newBuilder();
    pathElement.setId(element.getId());
    pathElement.setName(element.getName());
    pathElement.setKind(element.getType());
    return pathElement.build();
  }

  public static com.google.datastore.v1.AllocateIdsResponse translate(
      final AllocateIdsResponse response,
      final com.google.datastore.v1.AllocateIdsRequest request
  ) {
    final com.google.datastore.v1.AllocateIdsResponse.Builder allocateIdsResponse =
        com.google.datastore.v1.AllocateIdsResponse.newBuilder();
    final long idStart = response.getStart();
    final long idEnd = response.getEnd();
    long idNext = idStart;
    for (final com.google.datastore.v1.Key key : request.getKeysList()) {
      final com.google.datastore.v1.Key.Builder keyBuilder = com.google.datastore.v1.Key.newBuilder();
      keyBuilder.mergeFrom(key);
      for (final com.google.datastore.v1.Key.PathElement pathElement : keyBuilder.getPathList()) {
        //pathElement.
        //TODO fill in name/id for last? path element
      }
      allocateIdsResponse.addKeys(keyBuilder.build());
    }
    return allocateIdsResponse.build();
  }

  public static BeginTransactionRequest translate(final com.google.datastore.v1.BeginTransactionRequest request) {
    final BeginTransactionRequest beginTransactionRequest = new BeginTransactionRequest();
    beginTransactionRequest.setApp(request.getProjectId());
    beginTransactionRequest.setDatabaseId(request.getProjectId());
    return beginTransactionRequest.freeze();
  }

  public static com.google.datastore.v1.BeginTransactionResponse translate(final Transaction response) {
    final com.google.datastore.v1.BeginTransactionResponse.Builder beginTransactionResponse =
        com.google.datastore.v1.BeginTransactionResponse.newBuilder();
    beginTransactionResponse.setTransaction(ByteString.copyFrom(Longs.toByteArray(response.getHandle())));
    return beginTransactionResponse.build();
  }

  public static Transaction translate(final com.google.datastore.v1.CommitRequest request) {
    throw new UnsupportedOperationException();
  }

  public static com.google.datastore.v1.CommitResponse translate(final CommitResponse response) {
    throw new UnsupportedOperationException();
  }

  public static GetRequest translate(final com.google.datastore.v1.LookupRequest request) {
    final GetRequest getRequest = new GetRequest();
    getRequest.setStrong(ReadConsistency.STRONG==request.getReadOptions().getReadConsistency());
    final Transaction transaction = new Transaction();
    transaction.setHandle(Longs.fromByteArray(request.getReadOptions().getTransaction().toByteArray()));
    getRequest.setTransaction(transaction.freeze());
    for(final com.google.datastore.v1.Key key : request.getKeysList()) {
      getRequest.addKey(translate(key));
    }
    return getRequest.freeze();
  }

  public static PropertyValue translate(final com.google.datastore.v1.Value value) {
    final PropertyValue propertyValue = new PropertyValue();
    switch(value.getValueTypeCase()) {
      case NULL_VALUE:
        break;
      case BOOLEAN_VALUE:
        propertyValue.setBooleanValue(value.getBooleanValue());
        break;
      case INTEGER_VALUE:
        propertyValue.setInt64Value(value.getIntegerValue());
        break;
      case DOUBLE_VALUE:
        propertyValue.setDoubleValue(value.getDoubleValue());
        break;
      case TIMESTAMP_VALUE:
        throw new UnsupportedOperationException();
      case KEY_VALUE:
        propertyValue.setReferenceValue(translateVal(value.getKeyValue()));
        break;
      case STRING_VALUE:
        propertyValue.setStringValue(value.getStringValue());
        break;
      case BLOB_VALUE:
        throw new UnsupportedOperationException();
      case GEO_POINT_VALUE:
        throw new UnsupportedOperationException();
      case ENTITY_VALUE:
        throw new UnsupportedOperationException();
      case ARRAY_VALUE:
        throw new UnsupportedOperationException();
      case VALUETYPE_NOT_SET:
        throw new UnsupportedOperationException();
    }
    return propertyValue.freeze();
  }

  public static com.google.datastore.v1.Value translate(final OnestoreEntity.PropertyValue property) {
    final com.google.datastore.v1.Value.Builder value = com.google.datastore.v1.Value.newBuilder();
    if (property.hasBooleanValue()) {
      value.setBooleanValue(property.isBooleanValue());
    }
    if (property.hasDoubleValue()) {
      value.setDoubleValue(property.getDoubleValue());
    }
    if (property.hasInt64Value()) {
      value.setIntegerValue(property.getInt64Value());
    }
    if (property.hasPointValue()) {
      throw new UnsupportedOperationException();
    }
    if (property.hasReferenceValue()) {
      value.setKeyValue(translate(property.getReferenceValue()));
    }
    if (property.hasStringValue()) {
      value.setStringValue(property.getStringValue());
    }
    if (property.hasUserValue()) {
      throw new UnsupportedOperationException();
    }
    return value.build();
  }

  public static com.google.datastore.v1.Entity translate(final Reference key, final EntityProto entityProto) {
    final com.google.datastore.v1.Entity.Builder entity =
        com.google.datastore.v1.Entity.newBuilder();
    final Reference keyRef = key==null?entityProto.hasKey()?entityProto.getKey():null:key;
    if (keyRef!=null) {
      entity.setKey(translate(keyRef));
    }
    for (int i=0; i<entityProto.propertySize(); i++) {
      final Property property = entityProto.getProperty(i);
      entity.putProperties(property.getName(), translate(property.getValue()));
    }
    return entity.build();
  }

  public static com.google.datastore.v1.EntityResult translate(final Entity response) {
    final com.google.datastore.v1.EntityResult.Builder entityResult =
        com.google.datastore.v1.EntityResult.newBuilder();
    entityResult.setEntity(translate(response.hasKey()?response.getKey():null, response.getEntity()));
    entityResult.setVersion(response.getVersion());
    return entityResult.build();
  }

  public static com.google.datastore.v1.LookupResponse translate(final GetResponse response) {
    final com.google.datastore.v1.LookupResponse.Builder lookupResponse =
        com.google.datastore.v1.LookupResponse.newBuilder();
    for (int i=0; i<response.entitySize(); i++){
      lookupResponse.addFound(translate(response.getEntity(i)));
    }
    //lookupResponse.addMissing() //TODO
    for (int i=0; i<response.deferredSize(); i++){
      lookupResponse.addDeferred(translate(response.getDeferred(i)));
    }
    return lookupResponse.build();
  }

  public static Transaction translate(final com.google.datastore.v1.RollbackRequest request) {
    throw new UnsupportedOperationException();
  }

  public static com.google.datastore.v1.RollbackResponse translate(final VoidProto response) {
    throw new UnsupportedOperationException();
  }


  public static Property translate(
      final com.google.datastore.v1.PropertyReference propertyReference,
      final com.google.datastore.v1.Value value
  ) {
    final Property property = new Property();
    property.setName(propertyReference.getName());
    property.setValue(translate(value));
    return property.freeze();
  }

  public static List<Filter> filters(final List<Filter> results, final com.google.datastore.v1.Filter filter) {
    switch (filter.getFilterTypeCase()) {
      case COMPOSITE_FILTER:
        break;
      case PROPERTY_FILTER:
        final Filter filterResult = new Filter();
        filterResult.addProperty(translate(filter.getPropertyFilter().getProperty(), filter.getPropertyFilter().getValue()));
        filterResult.setOp(filter.getPropertyFilter().getOpValue());
        results.add(filterResult);
        break;
      case FILTERTYPE_NOT_SET:
        break;
    }
    return results;
  }

  public static Order translate(final com.google.datastore.v1.PropertyOrder propertyOrder) {
    final Order order = new Order();
    order.setProperty(propertyOrder.getProperty().getName());
    order.setDirection(propertyOrder.getDirectionValue());
    return order;
  }

  public static Query translate(final com.google.datastore.v1.RunQueryRequest request) {
    final Query query = new Query();
    query.setNameSpace(request.getPartitionId().getNamespaceId());
    query.setDatabaseId(request.getPartitionId().getProjectId());
    query.setStrong(ReadConsistency.STRONG==request.getReadOptions().getReadConsistency());
    final Transaction transaction = new Transaction();
    transaction.setHandle(Longs.fromByteArray(request.getReadOptions().getTransaction().toByteArray()));
    query.setTransaction(transaction.freeze());

    for(final com.google.datastore.v1.Projection projection : request.getQuery().getProjectionList()) {
      query.addPropertyName(projection.getProperty().getName());
    }
    query.setKind(request.getQuery().getKind(0).getName());
    for(final Filter filter : filters(Lists.newArrayList(), request.getQuery().getFilter()) ) {
      query.addFilter(filter);
    }
    for(final com.google.datastore.v1.PropertyOrder propertyOrder : request.getQuery().getOrderList()){
      query.addOrder(translate(propertyOrder));
    }
    query.setDistinct(request.getQuery().getDistinctOnCount()>0); // hmmm
    query.setCompiledCursor(decodeCursor(request.getQuery().getStartCursor()));
    query.setEndCompiledCursor(decodeCursor(request.getQuery().getEndCursor()));
    query.setOffset(request.getQuery().getOffset());
    query.setLimit(request.getQuery().getLimit().getValue());
    return query.freeze();
  }

  public static com.google.datastore.v1.RunQueryResponse translate(final QueryResult response) {
    final com.google.datastore.v1.RunQueryResponse.Builder runQueryResponse =
        com.google.datastore.v1.RunQueryResponse.newBuilder();
    final com.google.datastore.v1.QueryResultBatch.Builder batch =
        com.google.datastore.v1.QueryResultBatch.newBuilder();
    batch.setSkippedResults(response.getSkippedResults());
    batch.setSkippedCursor(encodeCursor(response.getSkippedResultsCompiledCursor()));
    com.google.datastore.v1.EntityResult.ResultType resultType = ResultType.PROJECTION;
    for (int i=0; i>response.resultSize(); i++) {
      final com.google.datastore.v1.EntityResult.Builder entityResult =
          com.google.datastore.v1.EntityResult.newBuilder();
      entityResult.setEntity(translate(null, response.getResult(i)));
      entityResult.setCursor(encodeCursor(response.getResultCompiledCursor(i)));
      entityResult.setVersion(response.getVersion(i));
      batch.addEntityResults(entityResult.build());
    }
    batch.setEntityResultType(resultType);
    batch.setEndCursor(encodeCursor(response.getCompiledCursor()));
    batch.setMoreResults(response.isMoreResults()?NOT_FINISHED:NO_MORE_RESULTS);
    batch.setSnapshotVersion(101); //??
    runQueryResponse.setBatch(batch.build());
    return runQueryResponse.build();
  }

  public static ByteString encodeCursor(final CompiledCursor compiledCursor) {
    return ByteString.copyFrom(compiledCursor.toByteArray());
  }

  public static CompiledCursor decodeCursor(final ByteString cursor) {
    try {
      return CompiledCursor.parser().parseFrom(cursor.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
