package io.datakernel;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeFile;
import io.datakernel.storage.streams.StreamKeyFilter;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;

public class FileExample {
	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
	private static final BufferSerializer<KeyValue<Integer, Set<String>>> KEY_VALUE_SERIALIZER = new BufferSerializer<KeyValue<Integer, Set<String>>>() {
		@Override
		public void serialize(ByteBuf output, KeyValue<Integer, Set<String>> item) {
			output.writeInt(item.getKey());
			output.writeInt(item.getValue().size());
			for (String value : item.getValue()) output.writeJavaUTF8(value);
		}

		@Override
		public KeyValue<Integer, Set<String>> deserialize(ByteBuf input) {
			final int key = input.readInt();
			final Set<String> values = new TreeSet<>();
			for (int i = 0, size = input.readInt(); i < size; i++) values.add(input.readJavaUTF8());
			return new KeyValue<>(key, values);
		}
	};

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	private static String toString(Eventloop eventloop, StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> toList = StreamConsumers.toList(eventloop);
		producer.streamTo(toList);
		eventloop.run();
		return toList.getList().toString();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		final Eventloop eventloop = Eventloop.create();
		final ExecutorService executorService = Executors.newFixedThreadPool(4);

		final StorageNode<Integer, Set<String>> storage = new StorageNodeIterator<>(eventloop, asList(
				ofValue(eventloop, newKeyValue(1, "a")),
				ofValue(eventloop, newKeyValue(2, "b")))
				.iterator());

		final Path storagePath = Paths.get(Files.createTempDir().getAbsolutePath());
		final StorageNodeFile<Integer, Set<String>, KeyValue<Integer, Set<String>>> fileStorage = new StorageNodeFile<>(eventloop,
				storagePath, executorService, 100, KEY_VALUE_SERIALIZER, TestUnion.getInstance().inputToAccumulator());

		for (int i = 0; i < 2; i++) {
			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			fileStorage.getSortedInput(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(syncCallback) {
				@Override
				protected void onResult(final StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
					storage.getSortedOutput(ALWAYS_TRUE, new ForwardingResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>(syncCallback) {
						@Override
						protected void onResult(StreamProducer<KeyValue<Integer, Set<String>>> producer) {
							producer.streamTo(listenableConsumer(consumer, syncCallback));
						}
					});
				}
			});

			eventloop.run();
			syncCallback.get();
			System.out.println("sync");

			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> sortedStreamCallback = ResultCallbackFuture.create();
			fileStorage.getSortedOutput(ALWAYS_TRUE, sortedStreamCallback);

			eventloop.run();
			System.out.println("sortedStream");
			System.out.println(toString(eventloop, sortedStreamCallback.get()));
		}

		executorService.shutdown();
	}

	private static class TestUnion extends StreamReducers.ReducerToAccumulator<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> {
		private static final TestUnion INSTANCE = new TestUnion();

		private TestUnion() {

		}

		static TestUnion getInstance() {
			return INSTANCE;
		}

		@Override
		public KeyValue<Integer, Set<String>> createAccumulator(Integer key) {
			return new KeyValue<Integer, Set<String>>(key, new TreeSet<String>());
		}

		@Override
		public KeyValue<Integer, Set<String>> accumulate(KeyValue<Integer, Set<String>> accumulator, KeyValue<Integer, Set<String>> value) {
			accumulator.getValue().addAll(value.getValue());
			return accumulator;
		}
	}

	private static class StorageNodeIterator<K, V> implements StorageNode<K, V> {
		private final Eventloop eventloop;
		private final Iterator<StreamProducer<KeyValue<K, V>>> producers;

		private StorageNodeIterator(Eventloop eventloop, Iterator<StreamProducer<KeyValue<K, V>>> producers) {
			this.eventloop = eventloop;
			this.producers = producers;
		}

		@Override
		public void getSortedOutput(Predicate<K> predicate, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
			final StreamKeyFilter<K, KeyValue<K, V>> filter = new StreamKeyFilter<>(eventloop, predicate, new Function<KeyValue<K, V>, K>() {
				@Override
				public K apply(KeyValue<K, V> input) {
					return input.getKey();
				}
			});
			producers.next().streamTo(filter.getInput());
			callback.setResult(filter.getOutput());
		}

		@Override
		public void getSortedInput(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
			throw new UnsupportedOperationException();
		}
	}
}
