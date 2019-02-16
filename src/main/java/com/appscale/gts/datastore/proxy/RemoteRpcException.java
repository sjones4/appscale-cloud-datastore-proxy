/**
 * Copyright 2019 AppScale Systems, Inc
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.appscale.gts.datastore.proxy;

import com.google.apphosting.utils.remoteapi.RemoteApiPb.Response;

/**
 *
 */
public class RemoteRpcException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public enum Type {
    Application,
    Exception,
    Java,
    Rpc,
  }

  private final Type type;

  public RemoteRpcException(final Type type, final String message) {
    super(message);
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public static Response throwIfError(final Response response) {
    if (response.hasApplicationError()) {
      throw new RemoteRpcException(Type.Application, response.getApplicationError().getDetail());
    }
    if (response.hasJavaException()) {
      throw new RemoteRpcException(Type.Java, response.getJavaException());
    }
    if (response.hasException()) {
      throw new RemoteRpcException(Type.Exception, response.getException());
    }
    if (response.hasRpcError()) {
      throw new RemoteRpcException(Type.Rpc, response.getRpcError().getDetail());
    }
    return response;
  }
}
