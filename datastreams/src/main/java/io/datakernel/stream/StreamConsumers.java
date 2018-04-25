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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers.Decorator.Context;

import java.util.EnumSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.stream.StreamCapability.LATE_BINDING;

public final class StreamConsumers {
	private StreamConsumers() {
	}

	/**
	 * {@link StreamConsumer} which closes immediately after wiring.
	 */
	static final class ClosingImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		private StreamLogger streamLogger = StreamLogger.of(this, logger);

		@Override
		public void setProducer(StreamProducer<T> producer) {
			streamLogger.logOpen();
			getCurrentEventloop().post(() -> {
				streamLogger.logClose();
				endOfStream.set(null);
			});
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
	 * {@link StreamConsumer} which closes with given error immediately after wiring.
	 */
	static final class ClosingWithErrorImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();
		private final Throwable exception;

		private StreamLogger streamLogger = StreamLogger.of(this, logger);

		ClosingWithErrorImpl(Throwable exception) {
			this.exception = exception;
		}

		@Override
		public void setProducer(StreamProducer<T> producer) {
			streamLogger.logOpen();
			getCurrentEventloop().post(() -> {
				streamLogger.logClose();
				endOfStream.setException(exception);
			});
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
	 * {@link StreamConsumer} which does nothing with data and completes when producer completes.
	 * Used to trigger producer processing when its data does not matter.
	 *
	 * @param <T> type of received data
	 */
	static final class IdleImpl<T> implements StreamConsumer<T> {
		private final SettableStage<Void> endOfStream = SettableStage.create();

		private StreamLogger streamLogger = StreamLogger.of(this, logger);

		@Override
		public void setProducer(StreamProducer<T> producer) {
			streamLogger.logOpen();
			producer.getEndOfStream().whenComplete((result, throwable) -> {
				streamLogger.logClose();
				endOfStream.set(result, throwable);
			});
			producer.produce(StreamDataReceiver.noop());
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
	 * {@link StreamConsumer} which sends received items into given Java 8 {@link Consumer} object.
	 * Used to create simple in-place consumers without suspension control.
	 *
	 * @param <T> type of received data
	 */
	static final class OfConsumerImpl<T> extends AbstractStreamConsumer<T> {
		private final Consumer<T> consumer;

		OfConsumerImpl(Consumer<T> consumer) {
			this.consumer = consumer;
		}

		@Override
		protected void onStarted() {
			getProducer().produce(consumer::accept);
		}

		@Override
		protected void onEndOfStream() {

		}

		@Override
		protected void onError(Throwable t) {

		}

		@Override
		public Set<StreamCapability> getCapabilities() {
			return EnumSet.of(LATE_BINDING);
		}
	}

	public interface Decorator<T> {
		interface Context {
			void suspend();

			void resume();

			void closeWithError(Throwable error);
		}

		StreamDataReceiver<T> decorate(Context context, StreamDataReceiver<T> dataReceiver);
	}

	public static <T> StreamConsumerModifier<T, T> decorator(Decorator<T> decorator) {
		return decorator(decorator, "<-decorated");
	}

	public static <T> StreamConsumerModifier<T, T> decorator(Decorator<T> decorator, String tag) {
		return consumer -> new ForwardingStreamConsumer<T>(consumer, tag) {
			final SettableStage<Void> endOfStream = SettableStage.create();

			{
				consumer.getEndOfStream().whenComplete(endOfStream::trySet);
			}

			@Override
			public void setProducer(StreamProducer<T> producer) {
				super.setProducer(new ForwardingStreamProducer<T>(producer, tag) {
					@SuppressWarnings("unchecked")
					@Override
					public void produce(StreamDataReceiver<T> dataReceiver) {
						StreamDataReceiver<T>[] dataReceiverHolder = new StreamDataReceiver[1];
						Context context = new Context() {
							final Eventloop eventloop = getCurrentEventloop();

							@Override
							public void suspend() {
								producer.suspend();
							}

							@Override
							public void resume() {
								eventloop.post(() -> producer.produce(dataReceiverHolder[0]));
							}

							@Override
							public void closeWithError(Throwable error) {
								endOfStream.trySetException(error);
							}
						};
						dataReceiverHolder[0] = decorator.decorate(context, dataReceiver);
						super.produce(dataReceiverHolder[0]);
					}
				});
			}

			@Override
			public Stage<Void> getEndOfStream() {
				return endOfStream;
			}
		};
	}

	public static <T> StreamConsumerModifier<T, T> errorDecorator(Function<T, Throwable> errorFunction) {
		return decorator((context, dataReceiver) ->
				item -> {
					Throwable error = errorFunction.apply(item);
					if (error == null) {
						dataReceiver.onData(item);
					} else {
						context.closeWithError(error);
					}
				}, "<-errorDecorated");
	}

	public static <T> StreamConsumerModifier<T, T> suspendDecorator(Predicate<T> predicate, Consumer<Context> resumer) {
		return suspendDecorator(predicate, resumer, "<-suspendDecorated");
	}

	public static <T> StreamConsumerModifier<T, T> suspendDecorator(Predicate<T> predicate, Consumer<Context> resumer, String tag) {
		return decorator((context, dataReceiver) ->
				item -> {
					dataReceiver.onData(item);

					if (predicate.test(item)) {
						context.suspend();
						resumer.accept(context);
					}
				}, tag);
	}

	public static <T> StreamConsumerModifier<T, T> suspendDecorator(Predicate<T> predicate) {
		return suspendDecorator(predicate, "<-predicateSuspension");
	}

	public static <T> StreamConsumerModifier<T, T> suspendDecorator(Predicate<T> predicate, String tag) {
		return suspendDecorator(predicate, Context::resume, tag);
	}

	public static <T> StreamConsumerModifier<T, T> oneByOne() {
		return suspendDecorator(item -> true, "<-oneByOneSuspension");
	}

	public static <T> StreamConsumerModifier<T, T> randomlySuspending(Random random, double probability) {
		return suspendDecorator(item -> random.nextDouble() < probability, "<-randomSuspension(" + probability + ')');
	}

	public static <T> StreamConsumerModifier<T, T> randomlySuspending(double probability) {
		return randomlySuspending(new Random(), probability);
	}

	public static <T> StreamConsumerModifier<T, T> randomlySuspending() {
		return randomlySuspending(0.5);
	}
}
