package io.datakernel;

import com.google.common.base.Predicate;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import io.datakernel.async.AssertingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.predicate.BloomFilter;
import io.datakernel.predicate.BloomFilterPredicate;
import io.datakernel.predicate.BloomFilterPredicateTypeAdapter;
import io.datakernel.predicate.HashFunction;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.storage.StorageNodeTreeMap;
import io.datakernel.storage.remote.StorageNodeRemoteClient;
import io.datakernel.storage.remote.StorageNodeRemoteServer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.google.common.collect.Sets.newTreeSet;
import static java.util.Collections.singletonList;

public class RemoteBloomExample {
	private static final int PORT = 12547;
	private static final HashFunction<Integer> SIMPLE_HASH = new HashFunction<Integer>() {
		@Override
		public BitSet hashCode(Integer item) {
			return BitSet.valueOf(Ints.toByteArray(item.hashCode()));
		}
	};

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
				return (TypeAdapter<T>) new BloomFilterPredicateTypeAdapter<>(singletonList(SIMPLE_HASH));
			}
			return null;
		}
	};

	public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
		final Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().registerTypeAdapterFactory(TYPE_ADAPTER_FACTORY).create();
		final Eventloop eventloop = Eventloop.create();
		final TreeMap<Integer, Set<String>> treeMap = new TreeMap<>();
		treeMap.put(1, newTreeSet(singletonList("1")));
		treeMap.put(2, newTreeSet(singletonList("2")));
		treeMap.put(3, newTreeSet(singletonList("3")));
		treeMap.put(4, newTreeSet(singletonList("4")));
		treeMap.put(5, newTreeSet(singletonList("5")));
		treeMap.put(6, newTreeSet(singletonList("6")));
		final StorageNodeTreeMap<Integer, Set<String>> treeStorage = new StorageNodeTreeMap<>(eventloop, treeMap, null);

		final StorageNodeRemoteServer<Integer, Set<String>> server = new StorageNodeRemoteServer<>(eventloop, treeStorage, gson, KEY_VALUE_SERIALIZER)
				.withListenPort(PORT);

		server.listen();

		final InetSocketAddress address = new InetSocketAddress(PORT);
		final StorageNodeRemoteClient<Integer, Set<String>> client = new StorageNodeRemoteClient<>(eventloop, address, gson, KEY_VALUE_SERIALIZER);

		final BloomFilter<Integer> bloomFilter = createBloomFilter(1, 4);

		client.getSortedStreamProducer(new BloomFilterPredicate<>(bloomFilter), new ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>() {
			@Override
			protected void onResult(StreamProducer<KeyValue<Integer, Set<String>>> result) {
				final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> toList = StreamConsumers.toList(eventloop);
				result.streamTo(toList);
				toList.setCompletionCallback(new AssertingCompletionCallback() {
					@Override
					protected void onComplete() {
						System.out.println(toList.getList());
					}
				});
			}

			@Override
			protected void onException(Exception e) {
				System.out.println("client getSortedStream onException");
				System.out.println(e.getMessage() == null ? e.getClass() : e.getMessage());

			}
		});

		eventloop.run();
	}

	private static BloomFilter<Integer> createBloomFilter(int... ints) {
		final BitSet bitSet = new BitSet();
		for (int value : ints) bitSet.or(BitSet.valueOf(Ints.toByteArray(value)));
		return new BloomFilter<>(bitSet, singletonList(SIMPLE_HASH));
	}
}
