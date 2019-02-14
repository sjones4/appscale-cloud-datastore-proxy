/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import com.google.appengine.repackaged.org.apache.commons.httpclient.HttpClient;
import com.google.appengine.repackaged.org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import com.google.appengine.repackaged.org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import com.google.appengine.repackaged.org.apache.commons.httpclient.methods.PostMethod;
import com.google.apphosting.utils.remoteapi.RemoteApiPb.Request;
import com.google.apphosting.utils.remoteapi.RemoteApiPb.Response;

/**
 * Remote RPC client
 */
class RemoteRpc implements Closeable {
  private static final AtomicLong ID = new AtomicLong(1000);

  private MultiThreadedHttpConnectionManager connectionManager;
  private HttpClient client;

  public RemoteRpc() {
    this.connectionManager = new MultiThreadedHttpConnectionManager();
    this.client = new HttpClient(this.connectionManager);
  }

  public void close() {
    this.connectionManager.shutdown();
  }

  public byte[] call(
      final String host,
      final int port,
      final String serviceName,
      final String methodName,
      final Map<String,String> headers,
      final byte[] payload
  ) {
    final String requestId = Long.toString(ID.incrementAndGet());
    final Request request = makeRequest(serviceName, methodName, payload, requestId);
    final Response response = getResponse(host, port, headers, request);
    if (response.hasJavaException()) {
      //TODO error handling
    }
    if (response.hasException()) {
      //TODO error handling
    }
    return response.getResponseAsBytes();
  }

  private Request makeRequest(
      final String serviceName,
      final String methodName,
      final byte[] payload,
      final String requestId
  ) {
    return new Request()
        .setServiceName(serviceName)
        .setMethod(methodName)
        .setRequestAsBytes(payload)
        .setRequestId(requestId);
  }

  private Response getResponse(
      final String host,
      final int port,
      final Map<String,String> headers,
      final Request request
  ) {
    byte[] requestBytes = request.toByteArray();

    final String path = "/";
    final PostMethod post = new PostMethod("http://" + host + ":" + port + path);
    int statusCode = 500;
    try {
      post.setRequestHeader("Content-Type", "application/octet-stream");
      post.setRequestHeader("ProtocolBufferType", "Request");
      if (headers!=null) {
        headers.forEach(post::setRequestHeader);
      }
      post.setRequestEntity(new ByteArrayRequestEntity(requestBytes));
      statusCode = this.client.executeMethod(post);

      if (statusCode != 200) {
        throw new RuntimeException("Non 200 response status code " + statusCode);
      } else {
        final Response parsedResponse = new Response();
        final boolean parsed = parsedResponse.parseFrom(post.getResponseBody());
        if (parsed && parsedResponse.isInitialized()) {
          return parsedResponse;
        } else {
          throw new RuntimeException("Could not parse response");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("IO error", e);
    }
  }
}
