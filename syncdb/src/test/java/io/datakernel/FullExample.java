package io.datakernel;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.async.*;
import io.datakernel.balancer.Balancer;
import io.datakernel.balancer.NextNodesBalancer;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.merger.Merger;
import io.datakernel.merger.MergerReducer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeTreeMap;
import io.datakernel.storage.remote.StorageNodeRemoteClient;
import io.datakernel.storage.remote.StorageNodeRemoteServer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static io.datakernel.storage.StreamMergeUtils.keyBalancer;
import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static java.util.Arrays.asList;

public class FullExample {
	private static final int START_PORT = 12457;
	private static final List<InetSocketAddress> addresses = asList(
			new InetSocketAddress(START_PORT), new InetSocketAddress(START_PORT + 1),
			new InetSocketAddress(START_PORT + 2), new InetSocketAddress(START_PORT + 3),
			new InetSocketAddress(START_PORT + 4), new InetSocketAddress(START_PORT + 5));

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
				return (TypeAdapter<T>) new TypeAdapter<CustomPredicate>() {
					@Override
					public void write(JsonWriter jsonWriter, CustomPredicate customPredicate) throws IOException {
						jsonWriter.beginArray();
						jsonWriter.endArray();
					}

					@Override
					public CustomPredicate read(JsonReader jsonReader) throws IOException {
						jsonReader.beginArray();
						jsonReader.endArray();
						return new CustomPredicate();
					}
				};
			}
			return null;
		}
	};
	private static CustomPredicate customAlwaysTruePredicate() {
		return new CustomPredicate();
	}

	private final Eventloop eventloop;
	private final Gson gson;
	private final BufferSerializer<KeyValue<Integer, Set<String>>> bufferSerializer;

	public FullExample(Eventloop eventloop, Gson gson, BufferSerializer<KeyValue<Integer, Set<String>>> bufferSerializer) {
		this.eventloop = eventloop;
		this.gson = gson;
		this.bufferSerializer = bufferSerializer;
	}

	public static void main(String[] args) throws IOException {
		final Eventloop eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		final Gson gson = new GsonBuilder().registerTypeAdapterFactory(TYPE_ADAPTER_FACTORY).create();
		new FullExample(eventloop, gson, KEY_VALUE_SERIALIZER).start();
	}

	@SafeVarargs
	private final StorageNodeTreeMap<Integer, Set<String>> createAndStartNode(InetSocketAddress address, Merger<KeyValue<Integer, Set<String>>> merger, KeyValue<Integer, Set<String>>... keyValues) throws IOException {
		final StorageNodeTreeMap<Integer, Set<String>> dataStorageTreeMap = new StorageNodeTreeMap<>(eventloop, map(keyValues), merger);
		final StorageNodeRemoteServer<Integer, Set<String>> remoteServer = new StorageNodeRemoteServer<>(eventloop, dataStorageTreeMap, gson, bufferSerializer)
				.withListenAddress(address);

		remoteServer.listen();
		return dataStorageTreeMap;
	}

	private static KeyValue<Integer, Set<String>> keyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	@SafeVarargs
	private static KeyValue<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>> keyValue(StorageNode<Integer, Set<String>> key, StorageNode<Integer, Set<String>>... value) {
		return new KeyValue<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>>(key, Sets.newHashSet(asList(value)));
	}

	@SafeVarargs
	private static TreeMap<Integer, Set<String>> map(final KeyValue<Integer, Set<String>>... keyValues) {
		return new TreeMap<Integer, Set<String>>() {{
			for (KeyValue<Integer, Set<String>> keyValue : keyValues) {
				put(keyValue.getKey(), keyValue.getValue());
			}
		}};
	}

	@SafeVarargs
	private static Map<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>> mapNodes(final KeyValue<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>>... keyValues) {
		return new HashMap<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>>() {{
			for (KeyValue<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>> keyValue : keyValues) {
				put(keyValue.getKey(), keyValue.getValue());
			}
		}};
	}

	public void start() throws IOException {
		MergerReducer<Integer, Set<String>, Void> merger = new MergerReducer<>(StreamReducers.<Integer, KeyValue<Integer, Set<String>>>mergeSortReducer());

		final StorageNode<Integer, Set<String>> n0 = createAndStartNode(addresses.get(0), merger, keyValue(1, "01"), keyValue(2, "02"));
		final StorageNode<Integer, Set<String>> n1 = createAndStartNode(addresses.get(1), merger, keyValue(3, "11"), keyValue(4, "12"));
		final StorageNode<Integer, Set<String>> n2 = createAndStartNode(addresses.get(2), merger, keyValue(5, "21"), keyValue(6, "22"));
		final StorageNode<Integer, Set<String>> n3 = createAndStartNode(addresses.get(3), merger, keyValue(7, "31"), keyValue(8, "32"));
		final StorageNode<Integer, Set<String>> n4 = createAndStartNode(addresses.get(4), merger, keyValue(9, "41"), keyValue(10, "42"));
		final StorageNode<Integer, Set<String>> n5 = createAndStartNode(addresses.get(5), merger, keyValue(11, "51"), keyValue(12, "52"));

		final StorageNodeRemoteClient<Integer, Set<String>> c0 = new StorageNodeRemoteClient<>(eventloop, addresses.get(0), gson, bufferSerializer);
		final StorageNodeRemoteClient<Integer, Set<String>> c1 = new StorageNodeRemoteClient<>(eventloop, addresses.get(1), gson, bufferSerializer);
		final StorageNodeRemoteClient<Integer, Set<String>> c2 = new StorageNodeRemoteClient<>(eventloop, addresses.get(2), gson, bufferSerializer);
		final StorageNodeRemoteClient<Integer, Set<String>> c3 = new StorageNodeRemoteClient<>(eventloop, addresses.get(3), gson, bufferSerializer);
		final StorageNodeRemoteClient<Integer, Set<String>> c4 = new StorageNodeRemoteClient<>(eventloop, addresses.get(4), gson, bufferSerializer);
		final StorageNodeRemoteClient<Integer, Set<String>> c5 = new StorageNodeRemoteClient<>(eventloop, addresses.get(5), gson, bufferSerializer);

		final List<StorageNode<Integer, Set<String>>> clients = Arrays.<StorageNode<Integer, Set<String>>>asList(c0, c1, c2, c3, c4, c5);

		final Map<StorageNode<Integer, Set<String>>, Set<StorageNode<Integer, Set<String>>>> peers = mapNodes(
				keyValue(c0, c1, c2),
				keyValue(c1, c2, c3),
				keyValue(c2, c3, c4),
				keyValue(c3, c4, c5),
				keyValue(c4, c5, c0),
				keyValue(c5, c0, c1));


		final Balancer<Integer, Set<String>> balancerByNodes = new NextNodesBalancer<>(eventloop, 2, clients);

		scheduleCycleConsolidation(balancerByNodes, c0, c1, c2, c3, c4, c5);
		scheduleCyclePrint(eventloop, n0, n1, n2, n3, n4, n5);

		eventloop.run();
	}

	@SafeVarargs
	private final void scheduleCycleConsolidation(final Balancer<Integer, Set<String>> balancerByNodes, final StorageNodeRemoteClient<Integer, Set<String>>... nodes) {
		eventloop.schedule(eventloop.currentTimeMillis() + 5000, new Runnable() {
			@Override
			public void run() {
				System.out.println("start consolidation");
				final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
				for (final StorageNodeRemoteClient<Integer, Set<String>> node : nodes) {
					asyncRunnables.add(new AsyncRunnable() {
						@Override
						public void run(final CompletionCallback callback) {
							getProducerTask(customAlwaysTruePredicate(), node).call(new AssertingResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>() {
								@Override
								protected void onResult(StreamProducer<KeyValue<Integer, Set<String>>> producer) {
									producer.streamTo(listenableConsumer(keyBalancer(eventloop, node, balancerByNodes), callback));
								}
							});
						}
					});
				}

				AsyncRunnables.runInParallel(eventloop, asyncRunnables).run(new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						System.out.println("finish consolidation");
						scheduleCycleConsolidation(balancerByNodes, nodes);
					}
				});
			}
		});
	}

	@SafeVarargs
	private static <V> void scheduleCyclePrint(final Eventloop eventloop, final StorageNode<Integer, V>... nodes) {
		eventloop.schedule(eventloop.currentTimeMillis() + 3000, new Runnable() {
			@Override
			public void run() {
				AsyncRunnables.runInSequence(eventloop, printStateTasks(eventloop, nodes)).run(new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						System.out.println(Strings.repeat("-", 80));
						scheduleCyclePrint(eventloop, nodes);
					}
				});
			}
		});
	}

	@SafeVarargs
	private static <V> List<AsyncRunnable> printStateTasks(final Eventloop eventloop, StorageNode<Integer, V>... nodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		for (int nodeId = 0; nodeId < nodes.length; nodeId++) {
			final StorageNode<Integer, V> node = nodes[nodeId];
			final int finalNodeId = nodeId;
			asyncRunnables.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					getStateTask(eventloop, Predicates.<Integer>alwaysTrue(), node).call(new ForwardingResultCallback<List<KeyValue<Integer, V>>>(callback) {
						@Override
						protected void onResult(List<KeyValue<Integer, V>> result) {
							prettyPrintState(result, finalNodeId);
							callback.setComplete();
						}
					});
				}
			});
		}
		return asyncRunnables;
	}

	private static <K extends Comparable<K>, V> void prettyPrintState(List<KeyValue<K, V>> result, final int nodeId) {
		Collections.sort(result, new Comparator<KeyValue<K, V>>() {
			@Override
			public int compare(KeyValue<K, V> o1, KeyValue<K, V> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
		System.out.printf("storage %d:%n", nodeId);
		System.out.println("\t" + result);
	}

	private static <K, V> AsyncCallable<StreamConsumer<KeyValue<K, V>>> getConsumerTask(final StorageNode<K, V> node) {
		return new AsyncCallable<StreamConsumer<KeyValue<K, V>>>() {
			@Override
			public void call(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
				node.getSortedInput(callback);
			}
		};
	}

	private static <K, V> AsyncCallable<StreamProducer<KeyValue<K, V>>> getProducerTask(final Predicate<K> predicate, final StorageNode<K, V> node) {
		return new AsyncCallable<StreamProducer<KeyValue<K, V>>>() {
			@Override
			public void call(ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
				node.getSortedOutput(predicate, callback);
			}
		};
	}

	private static <K, V> AsyncCallable<List<KeyValue<K, V>>> getStateTask(final Eventloop eventloop,
	                                                                       final Predicate<K> predicate,
	                                                                       final StorageNode<K, V> storageNode) {
		return new AsyncCallable<List<KeyValue<K, V>>>() {
			@Override
			public void call(final ResultCallback<List<KeyValue<K, V>>> callback) {
				getProducerTask(predicate, storageNode).call(new ForwardingResultCallback<StreamProducer<KeyValue<K, V>>>(callback) {
					@Override
					protected void onResult(StreamProducer<KeyValue<K, V>> producer) {
						final StreamConsumers.ToList<KeyValue<K, V>> consumerToList = StreamConsumers.toList(eventloop);
						producer.streamTo(consumerToList);
						consumerToList.setCompletionCallback(new AssertingCompletionCallback() {
							@Override
							protected void onComplete() {
								callback.setResult(consumerToList.getList());
							}
						});
					}
				});
			}
		};
	}

	public static class CustomPredicate implements Predicate<Integer> {

		@Override
		public boolean apply(Integer input) {
			return true;
		}

		@Override
		public String toString() {
			return "CustomPredicate";
		}

	}
}
