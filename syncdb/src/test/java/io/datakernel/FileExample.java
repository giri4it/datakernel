package io.datakernel;

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
import io.datakernel.storage.StorageNodeMerger;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
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

		final StorageNode<Integer, Set<String>> storage = new StorageNode<Integer, Set<String>>() {
			private final Iterator<StreamProducer<KeyValue<Integer, Set<String>>>> producers = asList(
					ofValue(eventloop, newKeyValue(1, "a")),
					ofValue(eventloop, newKeyValue(2, "b")))
					.iterator();

			@Override
			public void getSortedOutput(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				callback.setResult(producers.next());
			}

			@Override
			public void getSortedInput(ResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>> callback) {
				throw new UnsupportedOperationException();
			}
		};

		final StreamReducers.Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer =
				TestUnion.getInstance().inputToAccumulator();

		final StorageNodeFile<Integer, Set<String>, KeyValue<Integer, Set<String>>> fileStorage = new StorageNodeFile<>(eventloop,
				Paths.get(Files.createTempDir().getAbsolutePath()), executorService, 100, KEY_VALUE_SERIALIZER, reducer);

		final StorageNodeMerger<Integer, Set<String>> merger = new StorageNodeMerger<>(eventloop, reducer, asList(storage, fileStorage));

		{
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> sortedStreamCallback = ResultCallbackFuture.create();
			fileStorage.getSortedOutput(ALWAYS_TRUE, sortedStreamCallback);

			eventloop.run();
			System.out.println("sortedStream");
			sortedStreamCallback.get().streamTo(new AbstractStreamConsumer<KeyValue<Integer, Set<String>>>(eventloop) {
				@Override
				public StreamDataReceiver<KeyValue<Integer, Set<String>>> getDataReceiver() {
					return new StreamDataReceiver<KeyValue<Integer, Set<String>>>() {
						@Override
						public void onData(KeyValue<Integer, Set<String>> item) {
							System.out.println(item);
						}
					};
				}
			});
		}

		for (int i = 0; i < 2; i++) {
			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			merger.getSortedOutput(ALWAYS_TRUE, new ForwardingResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>(syncCallback) {
				@Override
				protected void onResult(final StreamProducer<KeyValue<Integer, Set<String>>> producer) {
					fileStorage.getSortedInput(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(syncCallback) {
						@Override
						protected void onResult(StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
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
}
