package io.datakernel;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.DataStorageFileReader;
import io.datakernel.storage.DataStorageFileWriter;
import io.datakernel.storage.HasSortedStreamProducer;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;

public class FileExample {
	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
	private static final Function<KeyValue<Integer, Set<String>>, Integer> KEY_FUNCTION = new Function<KeyValue<Integer, Set<String>>, Integer>() {
		@Override
		public Integer apply(KeyValue<Integer, Set<String>> input) {
			return input.getKey();
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

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	private static String toString(Eventloop eventloop, StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> toList = StreamConsumers.toList(eventloop);
		producer.streamTo(toList);
		eventloop.run();
		return toList.getList().toString();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		final Path currentStateFile = Paths.get(File.createTempFile("/home/vsavchuk/doc/currentStateFile", ".bin").getAbsolutePath());
		final Path nextStateFile = Paths.get(File.createTempFile("/home/vsavchuk/doc/nextStateFile", ".bin").getAbsolutePath());

		final Eventloop eventloop = Eventloop.create();
		final ExecutorService executorService = Executors.newFixedThreadPool(4);

		final HasSortedStreamProducer<Integer, Set<String>> storage = new HasSortedStreamProducer<Integer, Set<String>>() {
			private final Iterator<StreamProducer<KeyValue<Integer, Set<String>>>> producers = asList(
					ofValue(eventloop, newKeyValue(1, "a")),
					ofValue(eventloop, newKeyValue(2, "b")))
					.iterator();

			@Override
			public void getSortedStreamProducer(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				callback.setResult(producers.next());
			}
		};

		final DataStorageFileReader<Integer, Set<String>> fileStorageReader = new DataStorageFileReader<>(eventloop,
				currentStateFile, nextStateFile, executorService, 100, KEY_VALUE_SERIALIZER, KEY_FUNCTION
		);

		final StreamReducers.Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer =
				TestUnion.getInstance().inputToAccumulator();
		final List<? extends HasSortedStreamProducer<Integer, Set<String>>> peers = asList(storage, fileStorageReader);
		final DataStorageFileWriter<Integer, Set<String>, KeyValue<Integer, Set<String>>> fileStorageWriter =
				new DataStorageFileWriter<>(eventloop, nextStateFile, currentStateFile, executorService,
				KEY_VALUE_SERIALIZER, KEY_FUNCTION, peers, reducer, ALWAYS_TRUE);

		{
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> sortedStreamCallback = ResultCallbackFuture.create();
			fileStorageReader.getSortedStreamProducer(ALWAYS_TRUE, sortedStreamCallback);

			eventloop.run();
			System.out.println("sortedStream");
			System.out.println(toString(eventloop, sortedStreamCallback.get()));
		}

		for (int i = 0; i < 2; i++) {
			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			fileStorageWriter.synchronize(new ForwardingCompletionCallback(syncCallback) {
				@Override
				protected void onComplete() {
					fileStorageReader.changeFiles();
					syncCallback.setComplete();
				}
			});

			eventloop.run();
			syncCallback.get();
			System.out.println("sync");

			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> sortedStreamCallback = ResultCallbackFuture.create();
			fileStorageReader.getSortedStreamProducer(ALWAYS_TRUE, sortedStreamCallback);

			eventloop.run();
			System.out.println("sortedStream");
			System.out.println(toString(eventloop, sortedStreamCallback.get()));
		}

		executorService.shutdown();
	}

	private static class TestUnion extends StreamReducers.ReducerToAccumulator<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> {
		private static final TestUnion INSTANCE = new TestUnion();

		private TestUnion() {

		}

		static TestUnion getInstance() {
			return INSTANCE;
		}

		@Override
		public KeyValue<Integer, Set<String>> createAccumulator(Integer key) {
			return new KeyValue<Integer, Set<String>>(key, new TreeSet<String>());
		}

		@Override
		public KeyValue<Integer, Set<String>> accumulate(KeyValue<Integer, Set<String>> accumulator, KeyValue<Integer, Set<String>> value) {
			accumulator.getValue().addAll(value.getValue());
			return accumulator;
		}
	}
}
