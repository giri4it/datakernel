package io.datakernel;

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
import io.datakernel.balancer.NodeBalancer;
import io.datakernel.balancer.NodeSelector;
import io.datakernel.balancer.SelectedNodesFilteredBalancer;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.merger.Merger;
import io.datakernel.merger.MergerReducer;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.PredicateFactory;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeTreeMap;
import io.datakernel.storage.remote.StorageNodeRemoteClient;
import io.datakernel.storage.remote.StorageNodeRemoteServer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.balancer.NodeSelectors.selectAliveNodes;
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
	private final StorageNodeTreeMap<Integer, Set<String>> createTreeNode(Merger<KeyValue<Integer, Set<String>>> merger, final KeyValue<Integer, Set<String>>... values) {
		return new StorageNodeTreeMap<>(eventloop, new TreeMap<Integer, Set<String>>() {{
			for (KeyValue<Integer, Set<String>> keyValue : values) {
				put(keyValue.getKey(), keyValue.getValue());
			}
		}}, merger);
	}

	private StorageNodeRemoteServer<Integer, Set<String>> startServerNode(InetSocketAddress address, StorageNodeTreeMap<Integer, Set<String>> dataStorageTreeMap) throws IOException {
		final StorageNodeRemoteServer<Integer, Set<String>> remoteServer = new StorageNodeRemoteServer<>(eventloop, dataStorageTreeMap, gson, bufferSerializer)
				.withListenAddress(address);

		remoteServer.listen();
		return remoteServer;
	}

	private StorageNodeRemoteClient<Integer, Set<String>> createRemoteClient(final InetSocketAddress address) {
		return new StorageNodeRemoteClient<>(eventloop, address, gson, bufferSerializer);
	}

	private KeyValue<Integer, Set<String>> keyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	public void start() throws IOException {
		final MergerReducer<Integer, Set<String>, Void> merger = new MergerReducer<>(StreamReducers.<Integer, KeyValue<Integer, Set<String>>>mergeSortReducer());

		final HashMap<Integer, List<Integer>> keys = new HashMap<>();
		keys.put(0, newArrayList(1, 2));
		keys.put(1, newArrayList(3, 4));
		keys.put(2, newArrayList(5, 6));
		keys.put(3, newArrayList(7, 8));

		final List<StorageNodeTreeMap<Integer, Set<String>>> trees = Arrays.asList(
				createTreeNode(merger, keyValue(keys.get(0).get(0), "a"), keyValue(keys.get(0).get(1), "b")),
				createTreeNode(merger, keyValue(keys.get(1).get(0), "c"), keyValue(keys.get(1).get(1), "d")),
				createTreeNode(merger, keyValue(keys.get(2).get(0), "e"), keyValue(keys.get(2).get(1), "f")),
				createTreeNode(merger, keyValue(keys.get(3).get(0), "g"), keyValue(keys.get(3).get(1), "h")));

		final List<StorageNodeRemoteServer<Integer, Set<String>>> servers = Arrays.asList(
				startServerNode(addresses.get(0), trees.get(0)),
				startServerNode(addresses.get(1), trees.get(1)),
				startServerNode(addresses.get(2), trees.get(2)),
				startServerNode(addresses.get(3), trees.get(3)));

		final List<StorageNodeRemoteClient<Integer, Set<String>>> clients = Arrays.asList(
				createRemoteClient(addresses.get(0)),
				createRemoteClient(addresses.get(1)),
				createRemoteClient(addresses.get(2)),
				createRemoteClient(addresses.get(3)));

		final int duplicates = 2;
		final NodeBalancer<Integer, Set<String>> balancerByNodes = new SelectedNodesFilteredBalancer<>(eventloop,
				createNextAliveNodesSelector(clients, duplicates), previousAliveNodeKeysPredicate(keys, clients, duplicates));

		printStates(clients);

		syncStates(clients, balancerByNodes);
		printStates(clients);

		servers.get(1).close().whenComplete((aVoid, throwable) -> eventloop.breakEventloop());
		keys.get(2).addAll(keys.get(1));
		eventloop.run();

		syncStates(clients, balancerByNodes);
		printStates(clients);
	}

	private void syncStates(List<? extends StorageNode<Integer, Set<String>>> clients, NodeBalancer<Integer, Set<String>> balancerByNodes) {
		System.out.println("Start sync");
		AsyncRunnables.runInParallel(eventloop, streamToPeersTasks(clients, balancerByNodes)).run()
				.whenComplete(AsyncCallbacks.assertBiConsumer(aVoid -> {
					System.out.println("Finish sync");
					System.out.println(Strings.repeat("-", 80));
					eventloop.breakEventloop();
				}));

		eventloop.run();
	}

	private void printStates(List<? extends StorageNode<Integer, Set<String>>> clients) {
		AsyncRunnables.runInSequence(eventloop, printStateTasks(clients)).run()
				.whenComplete(AsyncCallbacks.assertBiConsumer(aVoid -> {
					System.out.println(Strings.repeat("-", 80));
					eventloop.breakEventloop();
				}));

		eventloop.run();
	}

	private NodeSelector<Integer, Set<String>> createNextAliveNodesSelector(final List<? extends StorageNode<Integer, Set<String>>> clients, final int duplicates) {
		return initNode -> selectAliveNodes(eventloop, clients).thenApply(result ->
				newArrayList(limit(skip(concat(result, result), result.indexOf(initNode) + 1), duplicates)));
	}

	private PredicateFactory<Integer, Set<String>> previousAliveNodeKeysPredicate(final HashMap<Integer, List<Integer>> map, final List<? extends StorageNode<Integer, Set<String>>> clients, final int duplicates) {
		return node -> SettableStage.immediateStage(Predicates.in(newArrayList(map.get(clients.indexOf(node))))::apply);
	}

	private List<AsyncRunnable> streamToPeersTasks(List<? extends StorageNode<Integer, Set<String>>> clients, final NodeBalancer<Integer, Set<String>> balancerByNodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		for (int i = 0; i < clients.size(); i++) {
			final StorageNode<Integer, Set<String>> node = clients.get(i);
			final int finalI = i;
			asyncRunnables.add(() -> {
				final SettableStage<Void> stage = SettableStage.create();
				getProducerTask(customAlwaysTruePredicate(), node).call().whenComplete((producer, throwable) -> {
					if (throwable == null) {
						balancerByNodes.getPeers(node).whenComplete(AsyncCallbacks.assertBiConsumer(result -> {
							final StreamConsumers.StreamConsumerListenable<KeyValue<Integer, Set<String>>> downstreamConsumer = listenableConsumer(result);
							producer.streamTo(downstreamConsumer);
							downstreamConsumer.getStage().whenComplete(AsyncCallbacks.forwardTo(stage));
						}));
					} else {
						System.out.println("ignore failed node with index: " + finalI);
						stage.setResult(null);
					}
				});
				return stage;
			});
		}
		return asyncRunnables;
	}

	private <V> List<AsyncRunnable> printStateTasks(List<? extends StorageNode<Integer, V>> nodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
			final StorageNode<Integer, V> node = nodes.get(nodeId);
			final int finalNodeId = nodeId;
			asyncRunnables.add(() -> getStateTask(customAlwaysTruePredicate(), node).call().whenComplete((result, throwable) -> {
				if (throwable == null) {
					prettyPrintState(result, finalNodeId);
				} else {
					prettyPrintState(Collections.<KeyValue<Integer, V>>emptyList(), finalNodeId);
				}
			}).thenApply(keyValues -> null));
		}
		return asyncRunnables;
	}

	private <K extends Comparable<K>, V> void prettyPrintState(List<KeyValue<K, V>> result, final int nodeId) {
		result.sort(Comparator.comparing(KeyValue::getKey));
		System.out.printf("storage %d:%n", nodeId);

		final Iterator<KeyValue<K, V>> iterator = result.iterator();
		if (!iterator.hasNext()) {
			System.out.println("\t no elements or exception");
			return;
		}

		final KeyValue<K, V> first = iterator.next();
		System.out.printf("\t[%2s: %s", first.getKey(), first.getValue());
		while (iterator.hasNext()) {
			final KeyValue<K, V> next = iterator.next();
			System.out.printf(", %2s: %s", next.getKey(), next.getValue());
		}
		System.out.println("]");
	}

	private <K, V> AsyncCallable<StreamProducer<KeyValue<K, V>>> getProducerTask(final Predicate<K> predicate, final StorageNode<K, V> node) {
		return () -> node.getSortedOutput(predicate);
	}

	private <K, V> AsyncCallable<List<KeyValue<K, V>>> getStateTask(final Predicate<K> predicate,
	                                                                final StorageNode<K, V> storageNode) {
		return () -> getProducerTask(predicate, storageNode).call().thenCompose(producer -> {
			final StreamConsumers.ToList<KeyValue<K, V>> consumerToList = StreamConsumers.toList(eventloop);
			producer.streamTo(consumerToList);

			return consumerToList.getCompletionStage().thenApply(aVoid -> consumerToList.getList());
		});
	}

	public static class CustomPredicate implements Predicate<Integer> {

		@Override
		public boolean test(Integer integer) {
			return true;
		}

		@Override
		public String toString() {
			return "CustomPredicate";
		}

	}
}
