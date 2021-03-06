/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.async.Promise;
import io.datakernel.exception.ParseException;
import io.datakernel.jmx.*;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcRemoteException;
import io.datakernel.rpc.protocol.RpcStream;
import io.datakernel.rpc.protocol.RpcStream.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

public final class RpcServerConnection implements Listener, JmxRefreshable {
	private static final Logger logger = LoggerFactory.getLogger(RpcServerConnection.class);

	private final RpcServer rpcServer;
	private final RpcStream stream;
	private final Map<Class<?>, RpcRequestHandler<?, ?>> handlers;

	private int activeRequests;
	private boolean readEndOfStream;

	// jmx
	private final InetAddress remoteAddress;
	private final ExceptionStats lastRequestHandlingException = ExceptionStats.create();
	private final ValueStats requestHandlingTime = ValueStats.create(RpcServer.SMOOTHING_WINDOW).withUnit("milliseconds");
	private EventStats successfulRequests = EventStats.create(RpcServer.SMOOTHING_WINDOW);
	private EventStats failedRequests = EventStats.create(RpcServer.SMOOTHING_WINDOW);
	private boolean monitoring = false;

	RpcServerConnection(RpcServer rpcServer, InetAddress remoteAddress,
			Map<Class<?>, RpcRequestHandler<?, ?>> handlers, RpcStream stream) {
		this.rpcServer = rpcServer;
		this.stream = stream;
		this.handlers = handlers;
		this.readEndOfStream = false;

		// jmx
		this.remoteAddress = remoteAddress;
	}

	@SuppressWarnings("unchecked")
	private Promise<Object> apply(Object request) {
		RpcRequestHandler<Object, Object> requestHandler = (RpcRequestHandler<Object, Object>) handlers.get(request.getClass());
		if (requestHandler == null) {
			return Promise.ofException(new ParseException(RpcServerConnection.class, "Failed to process request " + request));
		}
		return requestHandler.run(request);
	}

	@Override
	public void accept(RpcMessage message) {
		incrementActiveRequests();

		int cookie = message.getCookie();
		long startTime = monitoring ? System.currentTimeMillis() : 0;

		Object messageData = message.getData();
		apply(messageData).whenComplete((result, e) -> {
			if (startTime != 0) {
				int value = (int) (System.currentTimeMillis() - startTime);
				requestHandlingTime.recordValue(value);
				rpcServer.getRequestHandlingTime().recordValue(value);
			}
			if (e == null) {
				successfulRequests.recordEvent();
				rpcServer.getSuccessfulRequests().recordEvent();

				stream.sendMessage(RpcMessage.of(cookie, result));
				decrementActiveRequest();
			} else {
				lastRequestHandlingException.recordException(e, messageData);
				rpcServer.getLastRequestHandlingException().recordException(e, messageData);
				failedRequests.recordEvent();
				rpcServer.getFailedRequests().recordEvent();

				stream.sendMessage(RpcMessage.of(cookie, new RpcRemoteException(e)));
				decrementActiveRequest();
				logger.warn("Exception while processing request ID {}", cookie, e);
			}
		});
	}

	private void incrementActiveRequests() {
		activeRequests++;
	}

	private void decrementActiveRequest() {
		activeRequests--;
		if (readEndOfStream && activeRequests == 0) {
			stream.sendEndOfStream();
			onClosed();
		}
	}

	public void onClosed() {
		rpcServer.remove(this);
	}

	@Override
	public void onClosedWithError(Throwable e) {
		onClosed();

		// jmx
		String causedAddress = "Remote address: " + remoteAddress.getAddress().toString();
		logger.error("Protocol error. " + causedAddress, e);
		rpcServer.getLastProtocolError().recordException(e, causedAddress);
	}

	@Override
	public void onReadEndOfStream() {
		readEndOfStream = true;
		if (activeRequests == 0) {
			stream.sendEndOfStream();
			onClosed();
		}
	}

	public void close() {
		stream.sendCloseMessage();
	}

	// jmx
	public void startMonitoring() {
		monitoring = true;
	}

	public void stopMonitoring() {
		monitoring = false;
	}

	@JmxAttribute
	public boolean isOverloaded() {
		return stream.isOverloaded();
	}

	@JmxAttribute
	public EventStats getSuccessfulRequests() {
		return successfulRequests;
	}

	@JmxAttribute
	public EventStats getFailedRequests() {
		return failedRequests;
	}

	@JmxAttribute
	public ValueStats getRequestHandlingTime() {
		return requestHandlingTime;
	}

	@JmxAttribute
	public ExceptionStats getLastRequestHandlingException() {
		return lastRequestHandlingException;
	}

	@JmxAttribute
	public String getRemoteAddress() {
		return remoteAddress.toString();
	}

	@Override
	public void refresh(long timestamp) {
		successfulRequests.refresh(timestamp);
		failedRequests.refresh(timestamp);
		requestHandlingTime.refresh(timestamp);
	}
}
