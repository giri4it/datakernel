package io.datakernel;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.merger.Merger;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeTreeMap;
import io.datakernel.storage.remote.StorageNodeRemoteClient;
import io.datakernel.storage.remote.StorageNodeRemoteServer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

import static com.google.common.collect.Sets.newTreeSet;
import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static java.util.Collections.singletonList;

public class RemoteExample {
	private static final int PORT = 12547;
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

	private static final TypeAdapterFactory TYPE_ADAPTER_FACTORY = new TypeAdapterFactory() {
		@SuppressWarnings("unchecked")
		@Override
		public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
			if (Predicate.class.isAssignableFrom(typeToken.getRawType())) {
				return (TypeAdapter<T>) new TypeAdapter<EvenSortedStreamPredicate>() {
					@Override
					public void write(JsonWriter jsonWriter, EvenSortedStreamPredicate evenSortedStreamPredicate) throws IOException {
						jsonWriter.value(evenSortedStreamPredicate.getValue());
					}

					@Override
					public EvenSortedStreamPredicate read(JsonReader jsonReader) throws IOException {
						return new EvenSortedStreamPredicate(jsonReader.nextInt());
					}
				};
			}
			return null;
		}
	};

	private static class EvenSortedStreamPredicate implements Predicate<Integer> {
		private final int value;

		EvenSortedStreamPredicate(int value) {
			this.value = value;
		}

		@Override
		public boolean test(Integer integer) {
			return integer % getValue() == 0;
		}

		int getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "Predicate(n % " + value + ")";
		}

	}

	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
		final Gson gson = new GsonBuilder().registerTypeAdapterFactory(TYPE_ADAPTER_FACTORY).create();
		final Eventloop eventloop = Eventloop.create();
		final TreeMap<Integer, Set<String>> treeMap = new TreeMap<>();
		treeMap.put(1, newTreeSet(singletonList("1")));
		treeMap.put(2, newTreeSet(singletonList("2")));
		treeMap.put(3, newTreeSet(singletonList("3")));
		treeMap.put(4, newTreeSet(singletonList("4")));
		treeMap.put(5, newTreeSet(singletonList("5")));
		treeMap.put(6, newTreeSet(singletonList("6")));
		final StorageNodeTreeMap<Integer, Set<String>> treeStorage = new StorageNodeTreeMap<>(eventloop, treeMap, createUnionMerger());

		final StorageNodeRemoteServer<Integer, Set<String>> server = new StorageNodeRemoteServer<>(eventloop, treeStorage, gson, KEY_VALUE_SERIALIZER)
				.withListenPort(PORT);

		server.listen();

		final InetSocketAddress address = new InetSocketAddress(PORT);
		final StorageNodeRemoteClient<Integer, Set<String>> client = new StorageNodeRemoteClient<>(eventloop, address, gson, KEY_VALUE_SERIALIZER);
		final EvenSortedStreamPredicate evenPredicate = new EvenSortedStreamPredicate(2);

		printValues(evenPredicate, eventloop, client);

		eventloop.schedule(eventloop.currentTimeMillis() + 100, () -> client.getSortedInput().whenComplete((consumer, throwable) -> {
			if (throwable == null) {
				final KeyValue<Integer, Set<String>> value = new KeyValue<>(8, Sets.newTreeSet(singletonList("8")));
				final StreamConsumers.StreamConsumerListenable<KeyValue<Integer, Set<String>>> downstreamConsumer = listenableConsumer(consumer);
				StreamProducers.ofValue(eventloop, value).streamTo(downstreamConsumer);
				downstreamConsumer.getStage().whenComplete(AsyncCallbacks.assertBiConsumer(aVoid ->
						printValues(evenPredicate, eventloop, client)));
			} else {
				System.out.println("client getSortedInput onException");
				System.out.println(throwable.getMessage() == null ? throwable.getClass() : throwable.getMessage());
			}
		}));

		eventloop.run();
	}

	private static Merger<KeyValue<Integer, Set<String>>> createUnionMerger() {
		return (arg1, arg2) -> {
			if (arg2 == null || arg2.getValue() == null) return arg1;
			return new KeyValue<>(arg1.getKey(), newTreeSet(Iterables.concat(arg1.getValue(), arg2.getValue())));
		};
	}

	private static void printValues(Predicate<Integer> predicate, final Eventloop eventloop, StorageNodeRemoteClient<Integer, Set<String>> client) {
		client.getSortedOutput(predicate).whenComplete((result, throwable) -> {
			if (throwable == null) {
				final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> toList = StreamConsumers.toList(eventloop);
				result.streamTo(toList);
				toList.getCompletionStage().whenComplete(AsyncCallbacks.assertBiConsumer(aVoid -> System.out.println(toList.getList())));
			} else {
				System.out.println("client getSortedStream onException");
				System.out.println(throwable.getMessage() == null ? throwable.getClass() : throwable.getMessage());
			}
		});
	}
}
