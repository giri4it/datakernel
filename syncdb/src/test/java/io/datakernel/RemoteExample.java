package io.datakernel;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.async.AssertingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.DataStorageTreeMap;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.storage.remote.DataStorageRemoteClient;
import io.datakernel.storage.remote.DataStorageRemoteServer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.google.common.collect.Sets.newTreeSet;
import static java.util.Collections.singletonList;

public class RemoteExample {
	private static final int PORT = 12547;
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

	public static class EvenSortedStreamPredicate implements Predicate<Integer> {
		private final int value;

		public EvenSortedStreamPredicate(int value) {
			this.value = value;
		}

		@Override
		public boolean apply(Integer input) {
			return input % getValue() == 0;
		}

		public int getValue() {
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
		final DataStorageTreeMap<Integer, Set<String>> treeStorage = new DataStorageTreeMap<>(eventloop, treeMap, null);

		final DataStorageRemoteServer<Integer, Set<String>> server = new DataStorageRemoteServer<>(eventloop, treeStorage, gson, KEY_VALUE_SERIALIZER)
				.withListenPort(PORT);

		server.listen();

		final InetSocketAddress address = new InetSocketAddress(PORT);
		final DataStorageRemoteClient<Integer, Set<String>> client = new DataStorageRemoteClient<>(eventloop, address, gson, KEY_VALUE_SERIALIZER);

		client.getSortedStreamProducer(new EvenSortedStreamPredicate(2), new ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>>() {
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
				System.out.println(e);
			}
		});

		eventloop.run();
	}
}
