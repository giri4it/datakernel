package io.datakernel;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.async.SettableStage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeFile;
import io.datakernel.storage.StorageNodeTreeMap;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static java.util.Arrays.asList;

public class FileExample {
	private static final Predicate<Integer> ALWAYS_TRUE = integer -> true;
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
			final CompletableFuture<Void> future = fileStorage.getSortedInput()
					.thenCompose(consumer -> storageNode.getSortedOutput(ALWAYS_TRUE)
							.thenCompose(producer -> {
								final SettableStage<Void> stage = SettableStage.create();
								final StreamConsumers.StreamConsumerListenable<KeyValue<Integer, Set<String>>> downstreamConsumer = listenableConsumer(consumer);
								producer.streamTo(downstreamConsumer);
								downstreamConsumer.getStage().whenComplete(AsyncCallbacks.forwardTo(stage));
								return stage;
							})).toCompletableFuture();

			eventloop.run();
			future.get();
			System.out.println("sync");

			final CompletableFuture<StreamProducer<KeyValue<Integer, Set<String>>>> outputFuture = fileStorage
					.getSortedOutput(ALWAYS_TRUE).toCompletableFuture();

			eventloop.run();
			System.out.println("sortedStream");
			System.out.println(toString(eventloop, outputFuture.get()));
		}

		executorService.shutdown();
	}
}
