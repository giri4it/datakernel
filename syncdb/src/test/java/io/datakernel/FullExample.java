package io.datakernel;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.DataStorageTreeMap;
import io.datakernel.storage.HasSortedStream;
import io.datakernel.storage.HasSortedStream.KeyValue;
import io.datakernel.storage.remote.DataStorageRemoteClient;
import io.datakernel.storage.remote.DataStorageRemoteServer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class FullExample {
	private static final int START_PORT = 12457;
	private static final Predicate<Integer> EVEN_PREDICATE = new ModPredicate(2, false);
	private static final Predicate<Integer> ODD_PREDICATE = new ModPredicate(2, true);

	private static final List<InetSocketAddress> addresses = asList(
			new InetSocketAddress(START_PORT), new InetSocketAddress(START_PORT + 1),
			new InetSocketAddress(START_PORT + 2), new InetSocketAddress(START_PORT + 3));

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
				return (TypeAdapter<T>) new TypeAdapter<ModPredicate>() {
					@Override
					public void write(JsonWriter jsonWriter, ModPredicate modPredicate) throws IOException {
						jsonWriter.beginArray();
						jsonWriter.value(modPredicate.getValue());
						jsonWriter.value(modPredicate.isInvert());
						jsonWriter.endArray();
					}

					@Override
					public ModPredicate read(JsonReader jsonReader) throws IOException {
						jsonReader.beginArray();
						final int value = jsonReader.nextInt();
						final boolean invert = jsonReader.nextBoolean();
						jsonReader.endArray();
						return new ModPredicate(value, invert);
					}
				};
			}
			return null;
		}
	};

	private static <K extends Comparable<K>, V> DataStorageTreeMap<K, V, Void> createAndStartNode(
			final Eventloop eventloop, Gson gson, BufferSerializer<KeyValue<K, V>> bufferSerializer,
			int port, List<InetSocketAddress> addresses, Predicate<K> keyFilter,
			TreeMap<K, V> treeMap, StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, Void> reducer) throws IOException {

		final List<DataStorageRemoteClient<K, V>> remoteClients = new ArrayList<>();
		for (InetSocketAddress address : addresses) {
			remoteClients.add(new DataStorageRemoteClient<>(eventloop, address, gson, bufferSerializer, null, null));
		}

		final DataStorageTreeMap<K, V, Void> dataStorageTreeMap = new DataStorageTreeMap<>(eventloop, treeMap, remoteClients, reducer, keyFilter);
		final DataStorageRemoteServer<K, V> remoteServer = new DataStorageRemoteServer<>(eventloop, dataStorageTreeMap, gson, bufferSerializer)
				.withListenPort(port);

		remoteServer.listen();
		return dataStorageTreeMap;
	}

	private static KeyValue<Integer, Set<String>> keyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	@SafeVarargs
	private static TreeMap<Integer, Set<String>> map(final KeyValue<Integer, Set<String>>... keyValues) {
		return new TreeMap<Integer, Set<String>>() {{
			for (KeyValue<Integer, Set<String>> keyValue : keyValues) {
				put(keyValue.getKey(), keyValue.getValue());
			}
		}};
	}

	public static void main(String[] args) throws IOException {
		System.out.println("START");
		final Eventloop eventloop = Eventloop.create();
		final Gson gson = new GsonBuilder().registerTypeAdapterFactory(TYPE_ADAPTER_FACTORY).create();

		final StreamReducers.Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, Void> reducer =
				StreamReducers.mergeSortReducer();

		System.out.println("Start nodes");

		final TreeMap<Integer, Set<String>> treeMap0 = map(keyValue(1, "1:a"), keyValue(2, "2:a"), keyValue(3, "3:a"));
		final List<InetSocketAddress> addresses0 = asList(addresses.get(1), addresses.get(3));
		final DataStorageTreeMap<Integer, Set<String>, Void> node0 = createAndStartNode(eventloop, gson, KEY_VALUE_SERIALIZER,
				START_PORT, addresses0, EVEN_PREDICATE, treeMap0, reducer);

		final TreeMap<Integer, Set<String>> treeMap1 = new TreeMap<>();
		final List<InetSocketAddress> addresses1 = asList(addresses.get(0), addresses.get(2));
		final DataStorageTreeMap<Integer, Set<String>, Void> node1 = createAndStartNode(eventloop, gson, KEY_VALUE_SERIALIZER,
				START_PORT + 1, addresses1, ODD_PREDICATE, treeMap1, reducer);

		final TreeMap<Integer, Set<String>> treeMap2 = map(keyValue(3, "3:b"), keyValue(4, "4:a"), keyValue(5, "5:a"));
		final List<InetSocketAddress> addresses2 = asList(addresses.get(1), addresses.get(3));
		final DataStorageTreeMap<Integer, Set<String>, Void> node2 = createAndStartNode(eventloop, gson, KEY_VALUE_SERIALIZER,
				START_PORT + 2, addresses2, EVEN_PREDICATE, treeMap2, reducer);

		final TreeMap<Integer, Set<String>> treeMap3 = new TreeMap<>();
		final List<InetSocketAddress> addresses3 = asList(addresses.get(0), addresses.get(2));
		final DataStorageTreeMap<Integer, Set<String>, Void> node3 = createAndStartNode(eventloop, gson, KEY_VALUE_SERIALIZER,
				START_PORT + 3, addresses3, ODD_PREDICATE, treeMap3, reducer);

		System.out.println("All nodes started");

		schedulePrintAndSync(eventloop, node0, node1, node2, node3);

		eventloop.keepAlive(true);
		eventloop.run();

		System.out.println("FINISH");

	}

	private static void schedulePrintAndSync(final Eventloop eventloop, final DataStorageTreeMap<Integer, Set<String>, Void> node0, final DataStorageTreeMap<Integer, Set<String>, Void> node1, final DataStorageTreeMap<Integer, Set<String>, Void> node2, final DataStorageTreeMap<Integer, Set<String>, Void> node3) {
		eventloop.schedule(eventloop.currentTimeMillis() + 3000, new Runnable() {
			@Override
			public void run() {
				printState(eventloop, node0, node1, node2, node3, new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						syncState(eventloop, node0, node1, node2, node3);
						schedulePrintAndSync(eventloop, node0, node1, node2, node3);
					}
				});
			}
		});
	}

	private static void syncState(Eventloop eventloop, DataStorageTreeMap<Integer, Set<String>, Void> node0, DataStorageTreeMap<Integer, Set<String>, Void> node1, DataStorageTreeMap<Integer, Set<String>, Void> node2, DataStorageTreeMap<Integer, Set<String>, Void> node3) {
		System.out.println("Start nodes synchronization");
		AsyncRunnables.runInParallel(eventloop, asList(syncNode(node0),
				syncNode(node1),
				syncNode(node2),
				syncNode(node3)))
				.run(new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						System.out.println("All nodes synchronized");
					}
				});
	}

	private static AsyncRunnable syncNode(final DataStorageTreeMap<Integer, Set<String>, Void> node0) {
		return new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				node0.synchronize(callback);
			}
		};
	}

	private static void printState(final Eventloop eventloop,
	                               final DataStorageTreeMap<Integer, Set<String>, Void> node0,
	                               DataStorageTreeMap<Integer, Set<String>, Void> node1,
	                               DataStorageTreeMap<Integer, Set<String>, Void> node2,
	                               DataStorageTreeMap<Integer, Set<String>, Void> node3,
	                               final CompletionCallback callback) {
		AsyncCallables.callAll(eventloop, asList(getAsyncProducer(node0),
				getAsyncProducer(node1),
				getAsyncProducer(node2),
				getAsyncProducer(node3)))
				.call(new AssertingResultCallback<List<StreamProducer<KeyValue<Integer, Set<String>>>>>() {
					@Override
					protected void onResult(List<StreamProducer<KeyValue<Integer, Set<String>>>> producers) {
						AsyncCallables.callAll(eventloop, asList(
								getAsyncList(eventloop, producers.get(0)),
								getAsyncList(eventloop, producers.get(1)),
								getAsyncList(eventloop, producers.get(2)),
								getAsyncList(eventloop, producers.get(3))))
								.call(new ForwardingResultCallback<List<List<KeyValue<Integer, Set<String>>>>>(callback) {
									@Override
									protected void onResult(List<List<KeyValue<Integer, Set<String>>>> result) {
										for (int i = 0; i < result.size(); i++) {
											System.out.println("storage: " + i + " " + result.get(i));
										}
										callback.setComplete();
									}
								});
					}
				});
	}

	private static AsyncCallable<StreamProducer<KeyValue<Integer, Set<String>>>> getAsyncProducer(final DataStorageTreeMap<Integer, Set<String>, Void> node) {
		return new AsyncCallable<StreamProducer<KeyValue<Integer, Set<String>>>>() {
			@Override
			public void call(ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				node.getSortedStream(Predicates.<Integer>alwaysTrue(), callback);
			}
		};
	}

	private static AsyncCallable<List<KeyValue<Integer, Set<String>>>> getAsyncList(final Eventloop eventloop, final StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		return new AsyncCallable<List<KeyValue<Integer, Set<String>>>>() {
			@Override
			public void call(final ResultCallback<List<KeyValue<Integer, Set<String>>>> callback) {
				final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> consumerToList = StreamConsumers.toList(eventloop);
				producer.streamTo(consumerToList);
				consumerToList.setCompletionCallback(new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						callback.setResult(consumerToList.getList());
					}
				});
			}
		};
	}

	public static class ModPredicate implements Predicate<Integer> {
		private final int value;
		private final boolean invert;

		public ModPredicate(int value, boolean invert) {
			this.value = value;
			this.invert = invert;
		}

		@Override
		public boolean apply(Integer input) {
			return !invert ? input % getValue() == 0 : input % getValue() != 0;
		}

		public int getValue() {
			return value;
		}

		public boolean isInvert() {
			return invert;
		}

		@Override
		public String toString() {
			return "Predicate(n % " + value + ")";
		}

	}

}
