package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.async.AssertingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.storage.HasSortedStream.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

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

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Test
	public void testImmediateProducer() {
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final DataStorageListenable<Integer, String> dataStorage = new DataStorageListenable<>(eventloop, new HasSortedStream<Integer, String>() {
			@Override
			public void getSortedStream(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setResult(ofIterable(eventloop, data));
			}
		});

		final StreamConsumers.ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
		final StreamConsumers.ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

		eventloop.run();
		assertEquals(data, toList1.getList());
		assertEquals(data, toList2.getList());
	}

	@Test
	public void testScheduledProducer() {
		final long schedule = eventloop.currentTimeMillis() + 100;
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final DataStorageListenable<Integer, String> dataStorage = new DataStorageListenable<>(eventloop, new HasSortedStream<Integer, String>() {
			@Override
			public void getSortedStream(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				eventloop.schedule(schedule, new Runnable() {
					@Override
					public void run() {
						callback.setResult(ofIterable(eventloop, data));
					}
				});
			}
		});

		final StreamConsumers.ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
		final StreamConsumers.ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

		eventloop.run();
		assertEquals(data, toList1.getList());
		assertEquals(data, toList2.getList());
	}

	@Test
	public void testManyRequestAndAnswerIterations() {
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final DataStorageListenable<Integer, String> dataStorage = new DataStorageListenable<>(eventloop, new HasSortedStream<Integer, String>() {
			@Override
			public void getSortedStream(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setResult(ofIterable(eventloop, data));
			}
		});

		for (int i = 0; i < 4; i++) {
			final StreamConsumers.ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			final StreamConsumers.ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

			dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
			dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

			eventloop.run();
			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testExceptionOnGetSortedStream() {
		final Exception exception = new Exception("test exception");
		final DataStorageListenable<Integer, String> dataStorage = new DataStorageListenable<>(eventloop, new HasSortedStream<Integer, String>() {
			@Override
			public void getSortedStream(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setException(exception);
			}
		});

		final SaveExceptionCallback<StreamProducer<KeyValue<Integer, String>>> callback1 = new SaveExceptionCallback<>();
		final SaveExceptionCallback<StreamProducer<KeyValue<Integer, String>>> callback2 = new SaveExceptionCallback<>();
		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), callback1);
		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), callback2);

		eventloop.run();
		assertEquals(exception, callback1.getException());
		assertEquals(exception, callback2.getException());
	}

	@Test
	public void testProducerWithException() {
		final StreamProducer<KeyValue<Integer, String>> producer = StreamProducers.concat(eventloop,
				ofValue(eventloop, new KeyValue<>(1, "a")),
				StreamProducers.<KeyValue<Integer, String>>closingWithError(eventloop, new Exception("test exception")));

		final DataStorageListenable<Integer, String> dataStorage = new DataStorageListenable<>(eventloop, new HasSortedStream<Integer, String>() {
			@Override
			public void getSortedStream(Predicate<Integer> predicate, final ResultCallback<StreamProducer<KeyValue<Integer, String>>> callback) {
				callback.setResult(producer);
			}
		});

		final StreamConsumers.ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
		final StreamConsumers.ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList1));
		dataStorage.getSortedStream(Predicates.<Integer>alwaysTrue(), streamTo(toList2));

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

}