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
import io.datakernel.balancer.NodeSelectors;
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
	private final StorageNodeRemoteServer<Integer, Set<String>> startServerNode(InetSocketAddress address, Merger<KeyValue<Integer, Set<String>>> merger, KeyValue<Integer, Set<String>>... keyValues) throws IOException {
		final StorageNodeTreeMap<Integer, Set<String>> dataStorageTreeMap = new StorageNodeTreeMap<>(eventloop, map(keyValues), merger);
		final StorageNodeRemoteServer<Integer, Set<String>> remoteServer = new StorageNodeRemoteServer<>(eventloop, dataStorageTreeMap, gson, bufferSerializer)
				.withListenAddress(address);

		remoteServer.listen();
		return remoteServer;
	}

	private final StorageNode<Integer, Set<String>> createRemoteClient(InetSocketAddress address) {
		return new StorageNodeRemoteClient<>(eventloop, address, gson, bufferSerializer);
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

	public void start() throws IOException {
		final MergerReducer<Integer, Set<String>, Void> merger = new MergerReducer<>(StreamReducers.<Integer, KeyValue<Integer, Set<String>>>mergeSortReducer());

		final StorageNodeRemoteServer<Integer, Set<String>> s0 = startServerNode(addresses.get(0), merger, keyValue(1, "a"), keyValue(2, "b"));
		final StorageNodeRemoteServer<Integer, Set<String>> s1 = startServerNode(addresses.get(1), merger, keyValue(3, "c"), keyValue(4, "d"));
		final StorageNodeRemoteServer<Integer, Set<String>> s2 = startServerNode(addresses.get(2), merger, keyValue(5, "e"), keyValue(6, "f"));
		final StorageNodeRemoteServer<Integer, Set<String>> s3 = startServerNode(addresses.get(3), merger, keyValue(7, "g"), keyValue(8, "h"));
		final StorageNodeRemoteServer<Integer, Set<String>> s4 = startServerNode(addresses.get(4), merger, keyValue(9, "i"), keyValue(10, "j"));
		final StorageNodeRemoteServer<Integer, Set<String>> s5 = startServerNode(addresses.get(5), merger, keyValue(11, "k"), keyValue(12, "l"));

		final StorageNode<Integer, Set<String>> c0 = createRemoteClient(addresses.get(0));
		final StorageNode<Integer, Set<String>> c1 = createRemoteClient(addresses.get(1));
		final StorageNode<Integer, Set<String>> c2 = createRemoteClient(addresses.get(2));
		final StorageNode<Integer, Set<String>> c3 = createRemoteClient(addresses.get(3));
		final StorageNode<Integer, Set<String>> c4 = createRemoteClient(addresses.get(4));
		final StorageNode<Integer, Set<String>> c5 = createRemoteClient(addresses.get(5));

		final List<StorageNode<Integer, Set<String>>> clients = Arrays.asList(c0, c1, c2, c3, c4, c5);

		s3.close(breakEventloopCallback());
		eventloop.run();

		final int duplicates = 2;
		final NodeBalancer<Integer, Set<String>> balancerByNodes = new SelectedNodesFilteredBalancer<>(eventloop,
				createAliveNodesSelector(clients, duplicates), previousNodeKeysPredicate(clients, duplicates));

		AsyncRunnables.runInSequence(eventloop, printStateTasks(eventloop, clients)).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				System.out.println(Strings.repeat("-", 80));
				eventloop.breakEventloop();
			}
		});

		eventloop.run();

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

		AsyncRunnables.runInSequence(eventloop, printStateTasks(eventloop, clients)).run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
				System.out.println(Strings.repeat("-", 80));
				eventloop.breakEventloop();
			}
		});

		eventloop.run();
	}

	private NodeSelector<Integer, Set<String>> createAliveNodesSelector(final List<StorageNode<Integer, Set<String>>> clients, final int duplicates) {
		return new NodeSelector<Integer, Set<String>>() {
			@Override
			public void selectNodes(final StorageNode<Integer, Set<String>> initNode, final ResultCallback<Iterable<StorageNode<Integer, Set<String>>>> callback) {
				NodeSelectors.selectAliveNodes(eventloop, clients, new ForwardingResultCallback<Iterable<StorageNode<Integer, Set<String>>>>(callback) {
					@Override
					protected void onResult(Iterable<StorageNode<Integer, Set<String>>> result) {
						nextNodesSelector(duplicates, newArrayList(clients)).selectNodes(initNode, new ForwardingResultCallback<Iterable<StorageNode<Integer, Set<String>>>>(callback) {
							@Override
							protected void onResult(Iterable<StorageNode<Integer, Set<String>>> result) {
								callback.setResult(result);
							}
						});
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

	private NodeSelector<Integer, Set<String>> nextNodesSelector(final int duplicates, final List<StorageNode<Integer, Set<String>>> clients) {
		return new NodeSelector<Integer, Set<String>>() {
			@Override
			public void selectNodes(StorageNode<Integer, Set<String>> initNode, ResultCallback<Iterable<StorageNode<Integer, Set<String>>>> callback) {
				callback.setResult(limit(skip(concat(clients, clients), clients.indexOf(initNode) + 1), duplicates));
			}
		};
	}

	private PredicateFactory<Integer, Set<String>> previousNodeKeysPredicate(final List<StorageNode<Integer, Set<String>>> clients, final int duplicates) {
		return new PredicateFactory<Integer, Set<String>>() {
			@Override
			public Predicate<Integer> create(StorageNode<Integer, Set<String>> node) {
				final int nodeId = clients.indexOf(node);
				final List<Predicate<Integer>> predicates = new ArrayList<>();
				for (int i = 0; i < duplicates; i++) {
					final int producerNodeId = (nodeId - 1 - i + clients.size()) % clients.size();
					predicates.add(Predicates.in(asList(producerNodeId * 2 + 1, producerNodeId * 2 + 2)));
				}
				return Predicates.or(predicates);
			}
		};
	}

	private List<AsyncRunnable> streamToPeersTasks(List<StorageNode<Integer, Set<String>>> clients, final NodeBalancer<Integer, Set<String>> balancerByNodes) {
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

	private static <V> List<AsyncRunnable> printStateTasks(final Eventloop eventloop, List<StorageNode<Integer, V>> nodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		for (int nodeId = 0; nodeId < nodes.size(); nodeId++) {
			final StorageNode<Integer, V> node = nodes.get(nodeId);
			final int finalNodeId = nodeId;
			asyncRunnables.add(new AsyncRunnable() {
				@Override
				public void run(final CompletionCallback callback) {
					getStateTask(eventloop, customAlwaysTruePredicate(), node).call(new ResultCallback<List<KeyValue<Integer, V>>>() {
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

	private static <K extends Comparable<K>, V> void prettyPrintState(List<KeyValue<K, V>> result, final int nodeId) {
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
