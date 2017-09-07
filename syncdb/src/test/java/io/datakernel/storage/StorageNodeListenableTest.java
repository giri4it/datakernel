package io.datakernel.storage;

import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.*;
import io.datakernel.stream.StreamConsumers.ToList;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.util.Function;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StorageNodeListenableTest {

	private static <T> Predicate<T> alwaysTrue() {
		return t -> true;
	}

	private static <K, V> BiConsumer<StreamProducer<KeyValue<K, V>>, ? super Throwable> streamTo(final StreamConsumer<KeyValue<K, V>> consumer) {
		return AsyncCallbacks.assertBiConsumer(producer -> producer.streamTo(consumer));
	}

	private static <K, V> BiConsumer<StreamConsumer<KeyValue<K, V>>, ? super Throwable> streamFrom(final StreamProducer<KeyValue<K, V>> producer) {
		return AsyncCallbacks.assertBiConsumer(producer::streamTo);
	}

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Test
	public void testImmediateProducer() {
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final StorageNodeProducerOnly<Integer, String> producerOnly = new StorageNodeProducerOnly<>(value ->
				SettableStage.immediateStage(ofIterable(eventloop, data)));

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, producerOnly, null);

		for (int i = 0; i < 4; i++) {
			final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

			dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList1));
			dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList2));

			eventloop.run();
			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testImmediateConsumer() {
		final List<ToList<KeyValue<Integer, String>>> consumersToList = new ArrayList<>();
		final StorageNodeConsumerOnly<Integer, String> consumerOnly = new StorageNodeConsumerOnly<>(() -> {
			final ToList<KeyValue<Integer, String>> consumerToList = StreamConsumers.toList(eventloop);
			consumersToList.add(consumerToList);
			return SettableStage.immediateStage(consumerToList);
		});

		final StorageNodeListenable<Integer, String, Void> listenableStorage = new StorageNodeListenable<>(eventloop, consumerOnly, StreamReducers.<Integer, KeyValue<Integer, String>>mergeDeduplicateReducer());

		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));
		for (int i = 0; i < 2; i++) {
			listenableStorage.getSortedInput().whenComplete(streamFrom(ofIterable(eventloop, data)));
			listenableStorage.getSortedInput().whenComplete(streamFrom(ofIterable(eventloop, data)));

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

		final StorageNodeProducerOnly<Integer, String> producerOnly = new StorageNodeProducerOnly<>(value -> {
			final SettableStage<StreamProducer<KeyValue<Integer, String>>> stage = SettableStage.create();
			eventloop.schedule(schedule, () -> stage.setResult(ofIterable(eventloop, data)));
			return stage;
		});

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, producerOnly, null);

		for (int i = 0; i < 4; i++) {
			final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

			dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList1));
			dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList2));

			eventloop.run();
			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testScheduledConsumer() {
		final long schedule = eventloop.currentTimeMillis() + 100;
		final List<ToList<KeyValue<Integer, String>>> consumersToList = new ArrayList<>();
		final StorageNodeConsumerOnly<Integer, String> consumerOnly = new StorageNodeConsumerOnly<>(() -> {
			final SettableStage<StreamConsumer<KeyValue<Integer, String>>> stage = SettableStage.create();
			eventloop.schedule(schedule, () -> {
				final ToList<KeyValue<Integer, String>> consumerToList = StreamConsumers.toList(eventloop);
				consumersToList.add(consumerToList);
				stage.setResult(consumerToList);
			});
			return stage;
		});

		final StorageNodeListenable<Integer, String, Void> listenableStorage = new StorageNodeListenable<>(eventloop, consumerOnly, StreamReducers.<Integer, KeyValue<Integer, String>>mergeDeduplicateReducer());

		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));
		for (int i = 0; i < 2; i++) {
			listenableStorage.getSortedInput().whenComplete(streamFrom(ofIterable(eventloop, data)));
			listenableStorage.getSortedInput().whenComplete(streamFrom(ofIterable(eventloop, data)));

			eventloop.run();
			for (ToList<KeyValue<Integer, String>> consumerToList : consumersToList) {
				assertEquals(data, consumerToList.getList());
			}
		}
	}

	@Test
	public void testWaitUntilPreviousProducerFinished() {
		final List<KeyValue<Integer, String>> data = asList(new KeyValue<>(1, "a"), new KeyValue<>(2, "b"));

		final StorageNodeProducerOnly<Integer, String> producerOnly = new StorageNodeProducerOnly<>(value ->
				SettableStage.immediateStage(new ScheduleEndOfStreamProducer(eventloop, data)));

		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, producerOnly, null);

		for (int i = 0; i < 2; i++) {
			final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
			dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList1));

			final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);
			eventloop.schedule(eventloop.currentTimeMillis() + 100, () ->
					dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList2)));

			eventloop.run();

			assertEquals(data, toList1.getList());
			assertEquals(data, toList2.getList());
		}
	}

	@Test
	public void testExceptionOnGetSortedStream() {
		final Exception exception = new Exception("test exception");
		final StorageNodeProducerOnly<Integer, String> producerOnly = new StorageNodeProducerOnly<>(value -> SettableStage.immediateFailedStage(exception));
		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, producerOnly, null);

		final SaveExceptionConsumer<StreamProducer<KeyValue<Integer, String>>> consumer1 = new SaveExceptionConsumer<>();
		final SaveExceptionConsumer<StreamProducer<KeyValue<Integer, String>>> consumer2 = new SaveExceptionConsumer<>();
		dataStorage.getSortedOutput(alwaysTrue()).whenComplete(consumer1);
		dataStorage.getSortedOutput(alwaysTrue()).whenComplete(consumer2);

		eventloop.run();
		assertEquals(exception, consumer1.getError());
		assertEquals(exception, consumer2.getError());
	}

	@Test
	public void testProducerWithException() {
		final StreamProducer<KeyValue<Integer, String>> producer = StreamProducers.concat(eventloop,
				ofValue(eventloop, new KeyValue<>(1, "a")),
				StreamProducers.<KeyValue<Integer, String>>closingWithError(eventloop, new Exception("test exception")));

		final StorageNodeProducerOnly<Integer, String> producerOnly = new StorageNodeProducerOnly<>(value -> SettableStage.immediateStage(producer));
		final StorageNodeListenable<Integer, String, Void> dataStorage = new StorageNodeListenable<>(eventloop, producerOnly, null);

		final ToList<KeyValue<Integer, String>> toList1 = StreamConsumers.toList(eventloop);
		final ToList<KeyValue<Integer, String>> toList2 = StreamConsumers.toList(eventloop);

		dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList1));
		dataStorage.getSortedOutput(alwaysTrue()).whenComplete(streamTo(toList2));

		eventloop.run();
		assertEquals(CLOSED_WITH_ERROR, toList1.getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, toList2.getConsumerStatus());
	}

	private static class SaveExceptionConsumer<T> implements BiConsumer<T, Throwable> {
		private Throwable error;

		@Override
		public void accept(T t, Throwable throwable) {
			if (throwable == null) throw new AssertionError("should fail");
			error = throwable;
		}

		public Throwable getError() {
			return error;
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
			eventloop.schedule(eventloop.currentTimeMillis() + 500, this::sendEndOfStream);
		}
	}

	private final class StorageNodeProducerOnly<K, V> implements StorageNode<K, V> {
		private final Function<Predicate<K>, CompletionStage<StreamProducer<KeyValue<K, V>>>> fn;

		private StorageNodeProducerOnly(Function<Predicate<K>, CompletionStage<StreamProducer<KeyValue<K, V>>>> fn) {
			this.fn = fn;
		}

		@Override
		public CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(Predicate<K> predicate) {
			return fn.apply(predicate);
		}

		@Override
		public CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput() {
			throw new UnsupportedOperationException();
		}
	}

	private final class StorageNodeConsumerOnly<K, V> implements StorageNode<K, V> {
		private final Supplier<CompletionStage<StreamConsumer<KeyValue<K, V>>>> supplier;

		private StorageNodeConsumerOnly(Supplier<CompletionStage<StreamConsumer<KeyValue<K, V>>>> supplier) {
			this.supplier = supplier;
		}

		@Override
		public CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput() {
			return supplier.get();
		}

		@Override
		public CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(Predicate<K> predicate) {
			throw new UnsupportedOperationException();
		}
	}

}