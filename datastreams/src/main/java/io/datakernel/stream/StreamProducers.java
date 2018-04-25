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

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;
import static io.datakernel.util.Preconditions.checkNotNull;

@SuppressWarnings("StatementWithEmptyBody")
public final class StreamProducers {
	private StreamProducers() {
	}

	/**
	 * {@link StreamProducer} which produces nothing and closes immediately after wiring.
	 */
	static class ClosingImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		private StreamLogger streamLogger = StreamLogger.of(this, logger);

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			streamLogger.logOpen();
			getCurrentEventloop().post(() -> {
				streamLogger.logClose();
				endOfStream.set(null);
			});
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			streamLogger.logProduceRequest();
		}

		@Override
		public void suspend() {
			streamLogger.logSuspendRequest();
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public StreamLogger getStreamLogger() {
			return streamLogger;
		}

		@Override
		public void setStreamLogger(StreamLogger streamLogger) {
			this.streamLogger = streamLogger;
		}

		@Override
		public String toString() {
			return streamLogger.getTag();
		}
	}

	/**
	 * {@link StreamProducer} which produces nothing closes with given error immediately after wiring.
	 */
	static class ClosingWithErrorImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		private StreamLogger streamLogger = StreamLogger.of(this, logger);

		private final Throwable exception;

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			streamLogger.logOpen();
			getCurrentEventloop().post(() -> {
				streamLogger.logCloseWithError(exception);
				endOfStream.setException(exception);
			});
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			streamLogger.logProduceRequest();
		}

		@Override
		public void suspend() {
			streamLogger.logSuspendRequest();
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public StreamLogger getStreamLogger() {
			return streamLogger;
		}

		@Override
		public void setStreamLogger(StreamLogger streamLogger) {
			this.streamLogger = streamLogger;
		}

		@Override
		public String toString() {
			return streamLogger.getTag();
		}
	}

	/**
	 * {@link StreamProducer} which produces nothing and closes when consumer is closed.
	 */
	static final class IdleImpl<T> implements StreamProducer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		private StreamLogger streamLogger = StreamLogger.of(this, logger);

		@Override
		public void setConsumer(StreamConsumer<T> consumer) {
			streamLogger.logOpen();
			consumer.getEndOfStream().whenComplete((result, throwable) -> {
				if (throwable != null) {
					streamLogger.logCloseWithError(throwable);
				} else {
					streamLogger.logClose();
				}
				endOfStream.set(result, throwable);
			});
		}

		@Override
		public void produce(StreamDataReceiver<T> dataReceiver) {
			streamLogger.logProduceRequest();
		}

		@Override
		public void suspend() {
			streamLogger.logSuspendRequest();
		}

		@Override
		public Stage<Void> getEndOfStream() {
			return endOfStream;
		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}

		@Override
		public StreamLogger getStreamLogger() {
			return streamLogger;
		}

		@Override
		public void setStreamLogger(StreamLogger streamLogger) {
			this.streamLogger = streamLogger;
		}

		@Override
		public String toString() {
			return streamLogger.getTag();
		}
	}

	/**
	 * {@link StreamProducer} which will send all values from given supplier until it returns {@code null}.
	 */
	static class OfSupplierImpl<T> extends AbstractStreamProducer<T> {
		private final Supplier<T> supplier;

		OfSupplierImpl(Supplier<T> supplier) {
			this.supplier = supplier;
		}

		@Override
		protected void produce() {
			if (!isReceiverReady()) {
				return;
			}
			T item = supplier.get();
			while (item != null) {
				send(item);
				if (!isReceiverReady()) {
					return;
				}
				item = supplier.get();
			}
			sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
		}
	}

	/**
	 * {@link StreamProducer} which will send all values from given iterator.
	 */
	static class OfIteratorImpl<T> extends AbstractStreamProducer<T> {
		private final Iterator<T> iterator;

		OfIteratorImpl(Iterator<T> iterator) {
			this.iterator = checkNotNull(iterator);
		}

		@Override
		protected void produce() {
			while (iterator.hasNext()) {
				if (!isReceiverReady()) {
					return;
				}
				send(iterator.next());
			}
			sendEndOfStream();
		}

		@Override
		protected void onError(Throwable t) {
		}
	}

	public static <T> StreamProducerModifier<T, T> suppliedEndOfStream(Function<Stage<Void>, Stage<Void>> endOfStreamSupplier) {
		return producer -> new ForwardingStreamProducer<T>(producer, "suppliedEndOfStream") {
			final Stage<Void> endOfStream = endOfStreamSupplier.apply(producer.getEndOfStream());

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, T> suppliedEndOfStream(Stage<Void> suppliedEndOfStream) {
		return suppliedEndOfStream(actualEndOfStream -> Stages.any(actualEndOfStream, suppliedEndOfStream));
	}

	public interface Decorator<T> {
		interface Context {
			void endOfStream();

			void closeWithError(Throwable error);
		}

		StreamDataReceiver<T> decorate(Context context, StreamDataReceiver<T> dataReceiver);
	}

	public static <T> StreamProducerModifier<T, T> decorator(Decorator<T> decorator) {
		return decorator(decorator, "decorated");
	}

	public static <T> StreamProducerModifier<T, T> decorator(Decorator<T> decorator, String tag) {
		return producer -> new ForwardingStreamProducer<T>(producer, tag) {
			final SettableStage<Void> endOfStream = SettableStage.create();

			{
				producer.getEndOfStream().whenComplete(endOfStream::trySet);
			}

			@Override
			public void produce(StreamDataReceiver<T> dataReceiver) {
				producer.produce(decorator.decorate(new Decorator.Context() {
					@Override
					public void endOfStream() {
						endOfStream.trySet(null);
					}

					@Override
					public void closeWithError(Throwable error) {
						endOfStream.trySetException(error);
					}
				}, dataReceiver));
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, T> errorDecorator(Function<T, Throwable> errorFunction) {
		return decorator((context, dataReceiver) ->
			item -> {
				Throwable error = errorFunction.apply(item);
				if (error == null) {
					dataReceiver.onData(item);
				} else {
					context.closeWithError(error);
				}
			}, "errorDecorated");
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError(Predicate<Throwable> endOfStreamPredicate) {
		return producer -> new ForwardingStreamProducer<T>(producer, "endOfStreamOnError") {
			final SettableStage<Void> endOfStream = SettableStage.create();

			{
				producer.getEndOfStream().whenComplete(($, throwable) -> {
					if (throwable == null) {
						endOfStream.set(null);
					} else {
						if (endOfStreamPredicate.test(throwable)) {
							endOfStream.set(null);
						} else {
							endOfStream.setException(throwable);
						}
					}
				});
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamProducerModifier<T, T> endOfStreamOnError() {
		return endOfStreamOnError(throwable -> true);
	}

	public static <T> StreamProducerModifier<T, T> noEndOfStream() {
		return producer -> new ForwardingStreamProducer<T>(producer, "noEndOfStream") {
			final SettableStage<Void> endOfStream = SettableStage.create();

			{
				producer.getEndOfStream()
					.whenException(endOfStream::setException);
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

}
