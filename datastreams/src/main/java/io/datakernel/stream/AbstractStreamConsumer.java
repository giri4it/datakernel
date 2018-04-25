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

package io.datakernel.stream;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.stream.StreamStatus.*;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;

/**
 * It is basic implementation of {@link StreamConsumer}
 *
 * @param <T> type of received item
 */
public abstract class AbstractStreamConsumer<T> implements StreamConsumer<T> {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Eventloop eventloop = Eventloop.getCurrentEventloop();
	private final long createTick = eventloop.tick();

	@Nullable
	private StreamProducer<T> producer;

	private StreamStatus status = OPEN;

	@Nullable
	private Throwable exception;

	private final SettableStage<Void> endOfStream = SettableStage.create();

	protected StreamLogger streamLogger = StreamLogger.of(this, logger);

	@Override
	public final void setProducer(StreamProducer<T> producer) {
		checkNotNull(producer, "producer");
		checkState(this.producer == null, "Cannot set producer more than once");

		checkState(getCapabilities().contains(LATE_BINDING) || eventloop.tick() == createTick,
				LATE_BINDING_ERROR_MESSAGE, this);

		streamLogger.logOpen();

		this.producer = producer;
		onWired();
		producer.getEndOfStream()
				.thenRun(this::endOfStream)
				.whenException(this::closeWithError);
	}

	protected void onWired() {
		eventloop.post(this::onStarted);
	}

	protected void onStarted() {
	}

	public boolean isWired() {
		return producer != null;
	}

	public final StreamProducer<T> getProducer() {
		return checkNotNull(producer, "Consumer not yet bound to any producer");
	}

	@Nullable
	public final StreamProducer<T> getProducerOrNull() {
		return producer;
	}

	protected final void endOfStream() {
		if (status.isClosed())
			return;
		status = END_OF_STREAM;

		streamLogger.logClose();

		onEndOfStream();
		eventloop.post(this::cleanup);
		endOfStream.set(null);
	}

	protected abstract void onEndOfStream();

	public final void closeWithError(Throwable t) {
		if (status.isClosed())
			return;
		status = CLOSED_WITH_ERROR;
		exception = t;
		streamLogger.logCloseWithError(t);
		onError(t);
		eventloop.post(this::cleanup);
		endOfStream.setException(t);
	}

	protected abstract void onError(Throwable t);

	protected void cleanup() {
	}

	public final StreamStatus getStatus() {
		return status;
	}

	@Nullable
	public final Throwable getException() {
		return exception;
	}

	@Override
	public final Stage<Void> getEndOfStream() {
		return endOfStream;
	}

	@Override
	public final StreamLogger getStreamLogger() {
		return streamLogger;
	}

	@Override
	public final void setStreamLogger(StreamLogger streamLogger) {
		this.streamLogger = streamLogger;
	}

	/**
	 * This method is useful for stream transformers that might add some capability to the stream
	 */
	protected static Set<StreamCapability> addCapabilities(@Nullable StreamConsumer<?> consumer,
			StreamCapability capability, StreamCapability... capabilities) {
		EnumSet<StreamCapability> result = EnumSet.of(capability, capabilities);
		if (consumer != null) {
			result.addAll(consumer.getCapabilities());
		}
		return result;
	}

	@Override
	public String toString() {
		return streamLogger.getTag();
	}
}
