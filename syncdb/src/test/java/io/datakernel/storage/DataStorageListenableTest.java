package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.async.AssertingResultCallback;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.*;
import io.datakernel.stream.StreamConsumers.ToList;
import io.datakernel.stream.processor.StreamReducers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DataStorageListenableTest {

	private static <K, V> ResultCallback<StreamProducer<KeyValue<K, V>>> streamTo(final StreamConsumer<KeyValue<K, V>> consumer) {
		return new AssertingResultCallback<StreamProducer<KeyValue<K, V>>>() {
			@Override
			protected void onResult(StreamProducer<KeyValue<K, V>> producer) {
				producer.streamTo(consumer);
			}
		};
	}

	private static AssertingResultCallback<StreamConsumer<KeyValue<Integer, String>>> streamFrom(final StreamProducer<KeyValue<Integer, String>> producer) {
		return new AssertingResultCallback<StreamConsumer<KeyValue<Integer, String>>>() {
			@Override
			protected void onResult(StreamConsumer<KeyValue<Integer, String>> consumer) {
				producer.streamTo(consumer);
			}
		};
	}

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Test
	public void testImmediateProducer() {
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, new StorageNodeProducerOnly<Integer, String>() {
			@Override
			public void getSortedOutput(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setResult(ofIterable(eventloop, data));
			}
		}, null);

		for (int i = 0; i < 4; i++) {
			final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

			dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
			dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

			eventloop.run();
			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testImmediateConsumer() {
		final List<ToList<KeyValue<Integer, String>>> consumersToList = new ArrayList<>();
		final StorageNodeListenable<Integer, String, Void> listenableStorage = new StorageNodeListenable<>(eventloop, new StorageNodeConsumerOnly<Integer, String>() {
			@Override
			public void getSortedInput(ResultCallback<StreamConsumer<KeyValue<Integer, String>>> callback) {
				final ToList<KeyValue<Integer, String>> consumerToList = StreamConsumers.toList(eventloop);
				consumersToList.add(consumerToList);
				callback.setResult(consumerToList);
			}
		}, StreamReducers.<Integer, KeyValue<Integer, String>>mergeDeduplicateReducer());

		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));
		for (int i = 0; i < 2; i++) {
			listenableStorage.getSortedInput(streamFrom(ofIterable(eventloop, data)));
			listenableStorage.getSortedInput(streamFrom(ofIterable(eventloop, data)));

			eventloop.run();
			for (ToList<KeyValue<Integer, String>> consumerToList : consumersToList) {
				assertEquals(data, consumerToList.getList());
			}
		}
	}

	@Test
	public void testScheduledProducer() {
		final long schedule = eventloop.currentTimeMillis() + 100;
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, new StorageNodeProducerOnly<Integer, String>() {
			@Override
			public void getSortedOutput(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				eventloop.schedule(schedule, new Runnable() {
					@Override
					public void run() {
						callback.setResult(ofIterable(eventloop, data));
					}
				});
			}
		}, null);

		for (int i = 0; i < 4; i++) {
			final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

			dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
			dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

			eventloop.run();
			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testScheduledConsumer() {
		final long schedule = eventloop.currentTimeMillis() + 100;
		final List<ToList<KeyValue<Integer, String>>> consumersToList = new ArrayList<>();
		final StorageNodeListenable<Integer, String, Void> listenableStorage = new StorageNodeListenable<>(eventloop, new StorageNodeConsumerOnly<Integer, String>() {
			@Override
			public void getSortedInput(final ResultCallback<StreamConsumer<KeyValue<Integer, String>>> callback) {
				eventloop.schedule(schedule, new Runnable() {
					@Override
					public void run() {
						final ToList<KeyValue<Integer, String>> consumerToList = StreamConsumers.toList(eventloop);
						consumersToList.add(consumerToList);
						callback.setResult(consumerToList);
					}
				});
			}
		}, StreamReducers.<Integer, KeyValue<Integer, String>>mergeDeduplicateReducer());

		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));
		for (int i = 0; i < 2; i++) {
			listenableStorage.getSortedInput(streamFrom(ofIterable(eventloop, data)));
			listenableStorage.getSortedInput(streamFrom(ofIterable(eventloop, data)));

			eventloop.run();
			for (ToList<KeyValue<Integer, String>> consumerToList : consumersToList) {
				assertEquals(data, consumerToList.getList());
			}
		}
	}

	@Test
	public void testWaitUntilPreviousProducerFinished() {
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, new StorageNodeProducerOnly<Integer, String>() {

			@Override
			public void getSortedOutput(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setResult(new ScheduleEndOfStreamProducer(eventloop, data));
			}
		}, null);

		for (int i = 0; i < 2; i++) {
			final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList1));

			final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);
			eventloop.schedule(eventloop.currentTimeMillis() + 100, new Runnable() {
				@Override
				public void run() {
					dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList2));
				}
			});

			eventloop.run();

			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testExceptionOnGetSortedStream() {
		final Exception exception = new Exception("test exception");
		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, new StorageNodeProducerOnly<Integer, String>() {
			@Override
			public void getSortedOutput(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setException(exception);
			}
		}, null);

		final SaveExceptionCallback<StreamProducer<KeyValue<Integer, String>>> callback1 = new SaveExceptionCallback<>();
		final SaveExceptionCallback<StreamProducer<KeyValue<Integer, String>>> callback2 = new SaveExceptionCallback<>();
		dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), callback1);
		dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), callback2);

		eventloop.run();
		assertEquals(exception, callback1.getException());
		assertEquals(exception, callback2.getException());
	}

	@Test
	public void testProducerWithException() {
		final StreamProducer<KeyValue<Integer, String>> producer = StreamProducers.concat(eventloop,
				ofValue(eventloop, new KeyValue<>(1, "a")),
				StreamProducers.<KeyValue<Integer, String>>closingWithError(eventloop, new Exception("test exception")));

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, new StorageNodeProducerOnly<Integer, String>() {
			@Override
			public void getSortedOutput(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setResult(producer);
			}
		}, null);

		final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
		final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

		dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
		dataStorage.getSortedOutput(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

		eventloop.run();
		assertEquals(CLOSED_WITH_ERROR, toList1.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, toList2.getConsumerStatus());
	}

	private static class SaveExceptionCallback<T> extends ResultCallback<T> {
		private Exception exception;

		@Override
		protected void onResult(T result) {
			throw new AssertionError("should fail");
		}

		@Override
		protected void onException(Exception e) {
			exception = e;
		}

		public Exception getException() {
			return exception;
		}
	}

	private static class ScheduleEndOfStreamProducer extends AbstractStreamProducer<KeyValue<Integer, String>> {
		private final Iterable<KeyValue<Integer, String>> data;

		protected ScheduleEndOfStreamProducer(Eventloop eventloop, Iterable<KeyValue<Integer, String>> data) {
			super(eventloop);
			this.data = data;
		}

		@Override
		protected void onStarted() {
			for (KeyValue<Integer, String> item : data) send(item);
			eventloop.schedule(eventloop.currentTimeMillis() + 500, new Runnable() {
				@Override
				public void run() {
					sendEndOfStream();
				}
			});
		}
	}

	private static class ScheduleEndOfStreamConsumer<K, V> extends AbstractStreamConsumer<KeyValue<K, V>> {
		private final StreamConsumer<KeyValue<K, V>> consumer;
		private final CompletionCallback callback;

		protected ScheduleEndOfStreamConsumer(Eventloop eventloop, StreamConsumer<KeyValue<K, V>> consumer, CompletionCallback callback) {
			super(eventloop);
			this.consumer = consumer;
			this.callback = callback;
		}

		@Override
		public StreamDataReceiver<KeyValue<K, V>> getDataReceiver() {
			return consumer.getDataReceiver();
		}
	}

	private abstract class StorageNodeProducerOnly<K, V> implements StorageNode<K, V> {

		@Override
		public void getSortedInput(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
			throw new UnsupportedOperationException();
		}
	}

	private abstract class StorageNodeConsumerOnly<K, V> implements StorageNode<K, V> {

		@Override
		public void getSortedOutput(Predicate<K> predicate, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
			throw new UnsupportedOperationException();
		}
	}

}