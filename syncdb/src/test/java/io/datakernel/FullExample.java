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
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

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

	private StorageNodeRemoteClient<Integer, Set<String>> createRemoteClient(InetSocketAddress address) {
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
		keys.put(4, newArrayList(9, 10));
		keys.put(5, newArrayList(11, 12));

		final StorageNodeTreeMap<Integer, Set<String>> t0 = createTreeNode(merger, keyValue(keys.get(0).get(0), "a"), keyValue(keys.get(0).get(1), "b"));
		final StorageNodeTreeMap<Integer, Set<String>> t1 = createTreeNode(merger, keyValue(keys.get(1).get(0), "c"), keyValue(keys.get(1).get(1), "d"));
		final StorageNodeTreeMap<Integer, Set<String>> t2 = createTreeNode(merger, keyValue(keys.get(2).get(0), "e"), keyValue(keys.get(2).get(1), "f"));
		final StorageNodeTreeMap<Integer, Set<String>> t3 = createTreeNode(merger, keyValue(keys.get(3).get(0), "g"), keyValue(keys.get(3).get(1), "h"));
		final StorageNodeTreeMap<Integer, Set<String>> t4 = createTreeNode(merger, keyValue(keys.get(4).get(0), "i"), keyValue(keys.get(4).get(1), "j"));
		final StorageNodeTreeMap<Integer, Set<String>> t5 = createTreeNode(merger, keyValue(keys.get(5).get(0), "k"), keyValue(keys.get(5).get(1), "l"));

		final List<StorageNodeTreeMap<Integer, Set<String>>> trees = Arrays.asList(t0, t1, t2, t3, t4, t5);

		final StorageNodeRemoteServer<Integer, Set<String>> s0 = startServerNode(addresses.get(0), t0);
		final StorageNodeRemoteServer<Integer, Set<String>> s1 = startServerNode(addresses.get(1), t1);
		final StorageNodeRemoteServer<Integer, Set<String>> s2 = startServerNode(addresses.get(2), t2);
		final StorageNodeRemoteServer<Integer, Set<String>> s3 = startServerNode(addresses.get(3), t3);
		final StorageNodeRemoteServer<Integer, Set<String>> s4 = startServerNode(addresses.get(4), t4);
		final StorageNodeRemoteServer<Integer, Set<String>> s5 = startServerNode(addresses.get(5), t5);

		final StorageNodeRemoteClient<Integer, Set<String>> c0 = createRemoteClient(addresses.get(0));
		final StorageNodeRemoteClient<Integer, Set<String>> c1 = createRemoteClient(addresses.get(1));
		final StorageNodeRemoteClient<Integer, Set<String>> c2 = createRemoteClient(addresses.get(2));
		final StorageNodeRemoteClient<Integer, Set<String>> c3 = createRemoteClient(addresses.get(3));
		final StorageNodeRemoteClient<Integer, Set<String>> c4 = createRemoteClient(addresses.get(4));
		final StorageNodeRemoteClient<Integer, Set<String>> c5 = createRemoteClient(addresses.get(5));

		final List<StorageNodeRemoteClient<Integer, Set<String>>> clients = Arrays.asList(c0, c1, c2, c3, c4, c5);

		final int duplicates = 2;
		final NodeBalancer<Integer, Set<String>> balancerByNodes = new SelectedNodesFilteredBalancer<>(eventloop,
				createNextAliveNodesSelector(clients, duplicates), previousAliveNodeKeysPredicate(keys, clients, duplicates));
		{
			printStates(clients);
			syncStates(clients, balancerByNodes);
			printStates(clients);
		}

		{
			removeNewElements(keys, trees);
			printStates(clients);

			s3.close(breakEventloopCallback());
			eventloop.run();

			syncStates(clients, balancerByNodes);
			printStates(clients);
		}
	}

	private void removeNewElements(HashMap<Integer, List<Integer>> keys, List<StorageNodeTreeMap<Integer, Set<String>>> trees) {
		for (int i = 0; i < trees.size(); i++) {
			final StorageNodeTreeMap<Integer, Set<String>> tree = trees.get(i);
			for (Map.Entry<Integer, List<Integer>> entry : keys.entrySet()) {
				if (entry.getKey() != i) {
					for (Integer integer : entry.getValue()) {
						tree.remove(integer);
					}
				}
			}
		}
	}

	private void syncStates(List<? extends StorageNode<Integer, Set<String>>> clients, NodeBalancer<Integer, Set<String>> balancerByNodes) {
		System.out.println("Start sync");
		AsyncRunnables.runInParallel(eventloop, streamToPeersTasks(clients, balancerByNodes)).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				System.out.println("Finish sync");
				System.out.println(Strings.repeat("-", 80));
				eventloop.breakEventloop();
			}
		});

		eventloop.run();
	}

	private void printStates(List<? extends StorageNode<Integer, Set<String>>> clients) {
		AsyncRunnables.runInSequence(eventloop, printStateTasks(clients)).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				System.out.println(Strings.repeat("-", 80));
				eventloop.breakEventloop();
			}
		});

		eventloop.run();
	}

	private NodeSelector<Integer, Set<String>> createNextAliveNodesSelector(final List<? extends StorageNode<Integer, Set<String>>> clients, final int duplicates) {
		return new NodeSelector<Integer, Set<String>>() {
			@Override
			public void selectNodes(final StorageNode<Integer, Set<String>> initNode, final ResultCallback<List<StorageNode<Integer, Set<String>>>> callback) {
				selectAliveNodes(eventloop, clients, new ForwardingResultCallback<List<StorageNode<Integer, Set<String>>>>(callback) {
					@Override
					protected void onResult(List<StorageNode<Integer, Set<String>>> result) {
						final ArrayList<StorageNode<Integer, Set<String>>> result1 = newArrayList(limit(skip(concat(result, result), result.indexOf(initNode) + 1), duplicates));
						callback.setResult(result1);
					}
				});
			}
		};
	}

	private CompletionCallback breakEventloopCallback() {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				eventloop.breakEventloop();
			}

			@Override
			protected void onException(Exception e) {
				eventloop.breakEventloop();
			}
		};
	}

	private PredicateFactory<Integer, Set<String>> previousAliveNodeKeysPredicate(final HashMap<Integer, List<Integer>> map, final List<? extends StorageNode<Integer, Set<String>>> clients, final int duplicates) {
		return new PredicateFactory<Integer, Set<String>>() {
			@Override
			public void create(final StorageNode<Integer, Set<String>> node, final ResultCallback<Predicate<Integer>> callback) {
				selectAliveNodes(eventloop, clients, new ForwardingResultCallback<List<StorageNode<Integer, Set<String>>>>(callback) {
					@Override
					protected void onResult(List<StorageNode<Integer, Set<String>>> aliveNodes) {
						callback.setResult(Predicates.in(map.get(aliveNodes.indexOf(node))));
					}
				});
				callback.setResult(Predicates.<Integer>alwaysTrue());
			}
		};
	}

	private List<AsyncRunnable> streamToPeersTasks(List<? extends StorageNode<Integer, Set<String>>> clients, final NodeBalancer<Integer, Set<String>> balancerByNodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		for (int i = 0; i < clients.size(); i++) {
			final StorageNode<Integer, Set<String>> node = clients.get(i);
			final int finalI = i;
			asyncRunnables.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					getProducerTask(customAlwaysTruePredicate(), node).call(new ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>() {
						@Override
						protected void onResult(final StreamProducer<KeyValue<Integer, Set<String>>> producer) {
							balancerByNodes.getPeers(node, new AssertingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>() {
								@Override
								protected void onResult(StreamConsumer<KeyValue<Integer, Set<String>>> result) {
									producer.streamTo(listenableConsumer(result, callback));
								}
							});
						}

						@Override
						protected void onException(Exception e) {
							System.out.println("ignore failed node with index: " + finalI);
							callback.setComplete();
						}
					});
				}
			});
		}
		return asyncRunnables;
	}

	private <V> List<AsyncRunnable> printStateTasks(List<? extends StorageNode<Integer, V>> nodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
			final StorageNode<Integer, V> node = nodes.get(nodeId);
			final int finalNodeId = nodeId;
			asyncRunnables.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					getStateTask(customAlwaysTruePredicate(), node).call(new ResultCallback<List<KeyValue<Integer, V>>>() {
						@Override
						protected void onResult(List<KeyValue<Integer, V>> result) {
							prettyPrintState(result, finalNodeId);
							callback.setComplete();
						}

						@Override
						protected void onException(Exception e) {
							prettyPrintState(Collections.<KeyValue<Integer, V>>emptyList(), finalNodeId);
							callback.setComplete();
						}
					});
				}
			});
		}
		return asyncRunnables;
	}

	private <K extends Comparable<K>, V> void prettyPrintState(List<KeyValue<K, V>> result, final int nodeId) {
		Collections.sort(result, new Comparator<KeyValue<K, V>>() {
			@Override
			public int compare(KeyValue<K, V> o1, KeyValue<K, V> o2) {
				return o1.getKey().compareTo(o2.getKey());
			}
		});
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
		return new AsyncCallable<StreamProducer<KeyValue<K, V>>>() {
			@Override
			public void call(ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
				node.getSortedOutput(predicate, callback);
			}
		};
	}

	private <K, V> AsyncCallable<List<KeyValue<K, V>>> getStateTask(final Predicate<K> predicate,
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
