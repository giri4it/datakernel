package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.async.AssertingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.HasSortedStream.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.List;

import static io.datakernel.stream.StreamProducers.ofIterable;
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

	@Test
	public void testImmediateProducer() {
		final Eventloop eventloop = Eventloop.create();
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
		final Eventloop eventloop = Eventloop.create();
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
		final Eventloop eventloop = Eventloop.create();
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

}