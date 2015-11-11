/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.rpc.server;

import com.google.common.collect.ImmutableMap;
import io.datakernel.async.AsyncFunction;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.protocol.RpcMessage;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public final class RequestHandlers implements AsyncFunction<Object, Object> {

	public interface RequestHandler<R extends Object> {
		void run(R request, ResultCallback<Object> callback);
	}

	public static final class Builder {
		private final Map<Class<? extends Object>, RequestHandler<Object>> handlers = new HashMap<>();
		private Logger logger;

		@SuppressWarnings("unchecked")
		public <T extends Object> Builder put(Class<T> requestClass, RequestHandler<T> handler) {
			handlers.put(requestClass, (RequestHandler<Object>) handler);
			return this;
		}

		public Builder logger(Logger logger) {
			this.logger = logger;
			return this;
		}

		public RequestHandlers build() {
			return new RequestHandlers(ImmutableMap.copyOf(handlers), logger);
		}
	}

	private final ImmutableMap<Class<? extends Object>, RequestHandler<Object>> handlers;
	private final Logger logger;

	private RequestHandlers(ImmutableMap<Class<? extends Object>, RequestHandler<Object>> handlers, Logger logger) {
		this.handlers = handlers;
		this.logger = logger;
	}

	@Override
	public void apply(Object request, ResultCallback<Object> callback) {
		RequestHandler<Object> requestHandler;
		try {
			checkNotNull(request);
			checkNotNull(callback);
			requestHandler = handlers.get(request.getClass());
			checkNotNull(requestHandler, "Unknown request class: %", request.getClass());
		} catch (Exception e) {
			if (logger != null) {
				logger.error("Failed to process request " + request, e);
			}
			callback.onException(e);
			return;
		}
		requestHandler.run(request, callback);
	}
}
