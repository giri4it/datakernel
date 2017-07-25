package io.datakernel;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeFile;
import io.datakernel.storage.StorageNodeTreeMap;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;
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

	@SafeVarargs
	private static TreeMap<Integer, Set<String>> treeMap(KeyValue<Integer, Set<String>>... keyValues) {
		final TreeMap<Integer, Set<String>> map = new TreeMap<>();
		for (KeyValue<Integer, Set<String>> keyValue : keyValues) map.put(keyValue.getKey(), keyValue.getValue());
		return map;
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

		final Iterable<StorageNodeTreeMap<Integer, Set<String>>> storageNodes = asList(
				new StorageNodeTreeMap<>(eventloop, treeMap(newKeyValue(1, "a")), null),
				new StorageNodeTreeMap<>(eventloop, treeMap(newKeyValue(2, "b")), null));

		final Path storagePath = Paths.get(Files.createTempDir().getAbsolutePath());
		final StorageNodeFile<Integer, Set<String>, Void> fileStorage = new StorageNodeFile<>(eventloop,
				storagePath, executorService, 100, KEY_VALUE_SERIALIZER, StreamReducers.<Integer, KeyValue<Integer, Set<String>>>mergeSortReducer());

		for (final StorageNodeTreeMap<Integer, Set<String>> storageNode : storageNodes) {
			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			fileStorage.getSortedInput(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(syncCallback) {
				@Override
				protected void onResult(final StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
					storageNode.getSortedOutput(ALWAYS_TRUE, new ForwardingResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>(syncCallback) {
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
}
