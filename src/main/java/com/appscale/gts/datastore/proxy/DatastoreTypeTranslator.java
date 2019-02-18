/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NOT_FINISHED;
import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.NO_MORE_RESULTS;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.appengine.repackaged.com.google.common.primitives.Longs;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolMessage;
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
import com.google.datastore.v1.CommitRequest.TransactionSelectorCase;
import com.google.datastore.v1.MutationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.Timestamp;
import com.google.storage.onestore.v3.OnestoreEntity;
import com.google.storage.onestore.v3.OnestoreEntity.EntityProto;
import com.google.storage.onestore.v3.OnestoreEntity.Path;
import com.google.storage.onestore.v3.OnestoreEntity.Path.Element;
import com.google.storage.onestore.v3.OnestoreEntity.Property;
import com.google.storage.onestore.v3.OnestoreEntity.Property.Meaning;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue.ReferenceValue;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue.ReferenceValuePathElement;
import com.google.storage.onestore.v3.OnestoreEntity.PropertyValue.UserValue;
import com.google.storage.onestore.v3.OnestoreEntity.Reference;
import com.google.type.LatLng;

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
    reference.setApp(projectIdOrDefault(key.getPartitionId().getProjectId())); //TODO appid <-> project id conversions (appidentityservice?)
    reference.setNameSpace(key.getPartitionId().getNamespaceId());
    final Path path = new OnestoreEntity.Path();
    for (final com.google.datastore.v1.Key.PathElement pathElement : key.getPathList()) {
      path.addElement(translate(pathElement));
    }
    reference.setPath(path.freeze());
    return reference.freeze();
  }

  public static com.google.datastore.v1.Key translate(final Reference reference){
    final com.google.datastore.v1.Key.Builder keyBuilder = com.google.datastore.v1.Key.newBuilder();
    if (reference.hasDatabaseId() || reference.hasNameSpace()) {
      final com.google.datastore.v1.PartitionId.Builder partitionBuilder =
          com.google.datastore.v1.PartitionId.newBuilder();
      partitionBuilder.setProjectId(reference.getDatabaseId());
      partitionBuilder.setNamespaceId(reference.getNameSpace());
      keyBuilder.setPartitionId(partitionBuilder.build());
    }
    for (final Element element : reference.getPath().elements()) {
      keyBuilder.addPath(translate(element));
    }
    return keyBuilder.build();
  }

  public static ReferenceValue translateVal(final com.google.datastore.v1.Key key) {
    final ReferenceValue reference = new ReferenceValue();
    reference.setApp(projectIdOrDefault(key.getPartitionId().getProjectId()));
    reference.setNameSpace(key.getPartitionId().getNamespaceId());
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
    switch(pathElement.getIdTypeCase()) {
      case ID:
        element.setId(pathElement.getId());
        break;
      case NAME:
        element.setName(pathElement.getName());
        break;
    }
    element.setType(pathElement.getKind());
    return element.freeze();
  }

  public static com.google.datastore.v1.Key.PathElement translate(final Element element) {
    final com.google.datastore.v1.Key.PathElement.Builder pathElement =
        com.google.datastore.v1.Key.PathElement.newBuilder();
    if (element.hasId()) {
      pathElement.setId(element.getId());
    }
    if (element.hasName()) {
      pathElement.setName(element.getName());
    }
    pathElement.setKind(element.getType());
    return pathElement.build();
  }

  public static ReferenceValuePathElement translateElement(final com.google.datastore.v1.Key.PathElement pathElement) {
    final ReferenceValuePathElement element = new ReferenceValuePathElement();
    switch (pathElement.getIdTypeCase()) {
      case ID:
        element.setId(pathElement.getId());
        break;
      case NAME:
        element.setName(pathElement.getName());
        break;
    }
    element.setType(pathElement.getKind());
    return element.freeze();
  }

  public static com.google.datastore.v1.Key.PathElement translate(final ReferenceValuePathElement element) {
    final com.google.datastore.v1.Key.PathElement.Builder pathElement =
        com.google.datastore.v1.Key.PathElement.newBuilder();
    if (element.hasId()) {
      pathElement.setId(element.getId());
    }
    if (element.hasName()) {
      pathElement.setName(element.getName());
    }
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
    beginTransactionRequest.setApp(projectIdOrDefault(request.getProjectId()));
    return beginTransactionRequest.freeze();
  }

  public static com.google.datastore.v1.BeginTransactionResponse translate(final Transaction response) {
    final com.google.datastore.v1.BeginTransactionResponse.Builder beginTransactionResponse =
        com.google.datastore.v1.BeginTransactionResponse.newBuilder();
    beginTransactionResponse.setTransaction(encodeTx(response.getHandle()));
    return beginTransactionResponse.build();
  }

  public static List<ProtocolMessage<?>> translate(final com.google.datastore.v1.CommitRequest request) {
    final List<ProtocolMessage<?>> messages = Lists.newArrayList();
    final List<EntityProto> upserts = Lists.newArrayList();
    final List<Reference> deletes = Lists.newArrayList();
    for(final com.google.datastore.v1.Mutation mutation : request.getMutationsList()) {
      switch (mutation.getOperationCase()) {
        case INSERT:
          upserts.add(translate(mutation.getInsert()));
          break;
        case UPDATE:
          upserts.add(translate(mutation.getUpdate()));
          break;
        case UPSERT:
          upserts.add(translate(mutation.getUpsert()));
          break;
        case DELETE:
          deletes.add(translate(mutation.getDelete()));
          break;
        case OPERATION_NOT_SET:
          break;
      }
    }

    final Transaction tx;
    if (request.getTransactionSelectorCase() == TransactionSelectorCase.TRANSACTION) {
      tx = new Transaction()
          .setHandle(decodeTx(request.getTransaction()))
          .setApp(projectIdOrDefault(request.getProjectId()));
    } else {
      tx = null;
    }

    if ( !upserts.isEmpty() ) {
      final PutRequest put = new PutRequest();
      if (tx != null) {
        put.setTransaction(tx.clone());
      }
      upserts.forEach(put.mutableEntitys()::add);
      messages.add(put); // inserts
    }

    //TODO need to do this in a way that preserves entity order
    if ( !deletes.isEmpty() ) {
      final DeleteRequest delete = new DeleteRequest();
      if (tx != null) {
        delete.setTransaction(tx.clone());
      }
      deletes.forEach(delete.mutableKeys()::add);
      messages.add(delete); // deletes
    }

    if (tx != null) {
      messages.add(tx); // tx for commit
    }

    return messages;
  }

  public static com.google.datastore.v1.CommitResponse translate(
      final List<com.google.datastore.v1.MutationResult> results,
      final CommitResponse response) {
    final com.google.datastore.v1.CommitResponse.Builder commitResponse =
        com.google.datastore.v1.CommitResponse.newBuilder();
    commitResponse.addAllMutationResults(results);
    return commitResponse.build();
  }

  public static GetRequest translate(final com.google.datastore.v1.LookupRequest request) {
    final GetRequest getRequest = new GetRequest();
    getRequest.setStrong(
        com.google.datastore.v1.ReadOptions.ReadConsistency.STRONG==request.getReadOptions().getReadConsistency());
    if (request.hasReadOptions() && !request.getReadOptions().getTransaction().isEmpty()) {
      final Transaction transaction = new Transaction();
      transaction.setHandle(decodeTx(request.getReadOptions().getTransaction()));
      getRequest.setTransaction(transaction.freeze());
    }
    for(final com.google.datastore.v1.Key key : request.getKeysList()) {
      getRequest.addKey(translate(key));
    }
    return getRequest.freeze();
  }

  public static Property translate(final com.google.datastore.v1.Value value) {
    final Property property = new Property();
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
        property.setMeaning(Meaning.GD_WHEN);
        propertyValue.setInt64Value(
            (value.getTimestampValue().getSeconds() * 1_000_000) +
            (value.getTimestampValue().getNanos() / 1_000));
        break;
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
        if (value.getMeaning() == 20) {
          final com.google.datastore.v1.Entity entity = value.getEntityValue();
          final UserValue userValue = new UserValue();
          final com.google.datastore.v1.Value email = entity.getPropertiesOrDefault("email", null);
          final com.google.datastore.v1.Value authDomain = entity.getPropertiesOrDefault("auth_domain", null);
          final com.google.datastore.v1.Value id = entity.getPropertiesOrDefault("user_id", null);
          if (email != null) { userValue.setEmail(email.getStringValue()); }
          if (authDomain != null) { userValue.setAuthDomain(authDomain.getStringValue()); }
          if (id != null) { userValue.setObfuscatedGaiaid(id.getStringValue()); }
          propertyValue.setUserValue(userValue);
          break;
        } else {
          throw new UnsupportedOperationException();
        }
      case ARRAY_VALUE:
        throw new UnsupportedOperationException();
      case VALUETYPE_NOT_SET:
        throw new UnsupportedOperationException();
    }
    property.setValue(propertyValue.freeze());
    return property;
  }

  public static com.google.datastore.v1.Value translate(final OnestoreEntity.Property prop) {
    final OnestoreEntity.PropertyValue propValue = prop.getValue();
    final com.google.datastore.v1.Value.Builder value = com.google.datastore.v1.Value.newBuilder();
    if (propValue.hasBooleanValue()) {
      value.setBooleanValue(propValue.isBooleanValue());
    } else if (propValue.hasDoubleValue()) {
      value.setDoubleValue(propValue.getDoubleValue());
    } else if (propValue.hasInt64Value()) { // meaning == 7
      if (prop.getMeaning()== Meaning.GD_WHEN.getValue()) {
        final Timestamp.Builder builder = Timestamp.newBuilder()
            .setSeconds(propValue.getInt64Value() / 1_000_000L)
            .setNanos((int)(1000 * (propValue.getInt64Value() % 1_000_000L)));
        value.setTimestampValue(builder.build());
      } else {
        value.setIntegerValue(propValue.getInt64Value());
      }
    } else if (propValue.hasPointValue()) {
      final OnestoreEntity.PropertyValue.PointValue pointValue = propValue.getPointValue();
      final LatLng.Builder builder = LatLng.newBuilder();
      builder.setLatitude(pointValue.getX());
      builder.setLongitude(pointValue.getY());
      value.setGeoPointValue(builder.build());
    } else if (propValue.hasReferenceValue()) {
      value.setKeyValue(translate(propValue.getReferenceValue()));
    } else if (propValue.hasStringValue()) {
      value.setStringValue(propValue.getStringValue());
    } else if (propValue.hasUserValue()) {
      final UserValue userValue = propValue.getUserValue();
      final String userId = userValue.hasObfuscatedGaiaid() ? userValue.getObfuscatedGaiaid() : null;
      final com.google.datastore.v1.Entity.Builder builder = com.google.datastore.v1.Entity.newBuilder();
      final Function<String,com.google.datastore.v1.Value> unindexedValue = text ->
        com.google.datastore.v1.Value.newBuilder().setStringValue(text).setExcludeFromIndexes(true).build();
      builder.putProperties("email", unindexedValue.apply(userValue.getEmail()));
      builder.putProperties("auth_domain", unindexedValue.apply(userValue.getAuthDomain()));
      if (userId != null) {
        builder.putProperties("user_id", unindexedValue.apply(userId));
      }
      value.setEntityValue(builder.build());
      value.setMeaning(20);
    } else if (!prop.isMultiple()) {
      value.setNullValue(NullValue.NULL_VALUE);
    }
    //value.setExcludeFromIndexes(...)
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
      entity.putProperties(property.getName(), translate(property));
    }
    return entity.build();
  }

  public static Property translate(final String name, final com.google.datastore.v1.Value value) {
    final Property property = translate(value);
    property.setName(name);
    return property;
  }

  public static EntityProto translate(final com.google.datastore.v1.Entity entity) {
    final EntityProto entityProto = new EntityProto();
    entityProto.setKey(translate(entity.getKey()));
    for (final Map.Entry<String,com.google.datastore.v1.Value> propertyEntry : entity.getPropertiesMap().entrySet()) {
      entityProto.addProperty(translate(propertyEntry.getKey(), propertyEntry.getValue()));
    }
    return entityProto.freeze();
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

  public static List<com.google.datastore.v1.MutationResult> translate(final DeleteResponse response) {
    final List<com.google.datastore.v1.MutationResult> results = Lists.newArrayList();
    final List<Long> versions = response.versions();
    for (final long version : versions) {
      results.add(MutationResult.newBuilder().setVersion(version).build());
    }
    return results;
  }

  public static List<com.google.datastore.v1.MutationResult> translate(final PutResponse response) {
    final List<com.google.datastore.v1.MutationResult> results = Lists.newArrayList();
    final List<Reference> references = response.keys();
    final List<Long> versions = response.versions();
    for (int i=0; i<versions.size(); i++) {
      final long version = versions.get(i);
      final Reference reference = references.size() > i ? references.get(i) : null;
      final com.google.datastore.v1.MutationResult.Builder mutationResult =
          com.google.datastore.v1.MutationResult.newBuilder();
      mutationResult.setVersion(version);
      if (reference != null) {
        mutationResult.setKey(translate(reference));
      }
      results.add(mutationResult.build());
    }
    return results;
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
    final Property property = translate(value);
    property.setName(propertyReference.getName());
    return property.freeze();
  }

  public static List<Filter> filters(final List<Filter> results, final com.google.datastore.v1.Filter filter) {
    switch (filter.getFilterTypeCase()) {
      case COMPOSITE_FILTER:
        for(int i=0; i<filter.getCompositeFilter().getFiltersCount(); i++) {
          filters(results, filter.getCompositeFilter().getFilters(i));
        }
        break;
      case PROPERTY_FILTER:
        final Filter filterResult = new Filter();
        filterResult.addProperty(
            translate(filter.getPropertyFilter().getProperty(), filter.getPropertyFilter().getValue()));
        // datastore
        //        IN(6),
        //        EXISTS(7),
        //        CONTAINED_IN_REGION(8);
        //
        // cloud datastore v1
        //        HAS_ANCESTOR(11),
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

  private static String projectIdOrDefault(final String projectId) {
    return RemoteDatastoreService.getProjectIdOrContext(projectId);
  }

  public static Query translate(final com.google.datastore.v1.RunQueryRequest request) {
    final Query query = new Query();
    query.setNameSpace(request.getPartitionId().getNamespaceId());
    query.setApp(projectIdOrDefault(request.getPartitionId().getProjectId()));
    query.setStrong(
        com.google.datastore.v1.ReadOptions.ReadConsistency.STRONG==request.getReadOptions().getReadConsistency());
    if (request.hasReadOptions() && !request.getReadOptions().getTransaction().isEmpty()) {
      final Transaction transaction = new Transaction();
      transaction.setHandle(decodeTx(request.getReadOptions().getTransaction()));
      query.setTransaction(transaction.freeze());
    }
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
    if (!request.getQuery().getStartCursor().isEmpty()) {
      query.setCompiledCursor(decodeCursor(request.getQuery().getStartCursor()));
    }
    if (!request.getQuery().getEndCursor().isEmpty()) {
      query.setEndCompiledCursor(decodeCursor(request.getQuery().getEndCursor()));
    }
    if (query.hasOffset()) {
      query.setOffset(request.getQuery().getOffset());
    }
    if (query.hasLimit()) {
      query.setLimit(request.getQuery().getLimit().getValue());
    }
    return query.freeze();
  }

  public static com.google.datastore.v1.RunQueryResponse translate(final QueryResult response) {
    final com.google.datastore.v1.RunQueryResponse.Builder runQueryResponse =
        com.google.datastore.v1.RunQueryResponse.newBuilder();
    final com.google.datastore.v1.QueryResultBatch.Builder batch =
        com.google.datastore.v1.QueryResultBatch.newBuilder();
    batch.setSkippedResults(response.getSkippedResults());
    batch.setSkippedCursor(encodeCursor(response.getSkippedResultsCompiledCursor()));
    com.google.datastore.v1.EntityResult.ResultType resultType =
        com.google.datastore.v1.EntityResult.ResultType.PROJECTION;
    for (int i=0; i<response.resultSize(); i++) {
      final com.google.datastore.v1.EntityResult.Builder entityResult =
          com.google.datastore.v1.EntityResult.newBuilder();
      entityResult.setEntity(translate(null, response.getResult(i)));
      //entityResult.setCursor(encodeCursor(response.getResultCompiledCursor(i)));
      //entityResult.setVersion(response.getVersion(i));
      batch.addEntityResults(entityResult.build());
    }
    batch.setEntityResultType(resultType);
    batch.setEndCursor(encodeCursor(response.getCompiledCursor()));
    batch.setMoreResults(response.isMoreResults()?NOT_FINISHED:NO_MORE_RESULTS);
    batch.setSnapshotVersion(101); //??
    runQueryResponse.setBatch(batch.build());
    return runQueryResponse.build();
  }

  public static ByteString encodeTx(final long tx) {
    return ByteString.copyFrom(Longs.toByteArray(tx));
  }

  public static long decodeTx(final ByteString tx) {
    return Longs.fromByteArray(tx.toByteArray());
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
