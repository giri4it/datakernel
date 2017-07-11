package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.HasSortedStream.KeyValue;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamReducers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class DataStorageFileTest {
	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
	private static final List<? extends HasSortedStream<Integer, Set<String>>> EMPTY_PEERS = new ArrayList<>();
	private static final Function<KeyValue<Integer, Set<String>>, Integer> KEY_FUNCTION = new Function<KeyValue<Integer, Set<String>>, Integer>() {
		@Override
		public Integer apply(KeyValue<Integer, Set<String>> input) {
			return input.getKey();
		}
	};
	private static final BufferSerializer<KeyValue<Integer, Set<String>>> SERIALIZER = new BufferSerializer<KeyValue<Integer, Set<String>>>() {
		@Override
		public void serialize(ByteBuf output, KeyValue<Integer, Set<String>> item) {
			output.writeInt(item.getKey());
			output.writeInt(item.getValue().size());
			for (String value : item.getValue()) output.writeJavaUTF8(value);
		}

		@Override
		public KeyValue<Integer, Set<String>> deserialize(ByteBuf input) {
			final int key = input.readInt();
			final Set<String> treeSet = new TreeSet<>();
			for (int i = 0, len = input.readInt(); i < len; i++) treeSet.add(input.readJavaUTF8());
			return new KeyValue<>(key, treeSet);
		}
	};

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private Eventloop eventloop;
	private Path currentStateFile;
	private Path nextStateFile;
	private ExecutorService executorService;
	private DataStorageFile<Integer, Set<String>, Void> fileStorage;
	private StreamReducers.Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, Void> mergeReducer;

	private BufferSerializer<KeyValue<Integer, Set<String>>> serializer = SERIALIZER;
	private Function<KeyValue<Integer, Set<String>>, Integer> keyFunction = KEY_FUNCTION;
	private List<? extends HasSortedStream<Integer, Set<String>>> peers = EMPTY_PEERS;
	private Predicate<Integer> alwaysTrue = ALWAYS_TRUE;
	private int bufferSize = 1;

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	private static HasSortedStream<Integer, Set<String>> wrapHasSortedStream(final StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		return new HasSortedStream<Integer, Set<String>>() {
			@Override
			public void getSortedStream(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				callback.setResult(producer);
			}
		};
	}

	@Before
	public void before() throws IOException {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		mergeReducer = StreamReducers.mergeSortReducer();
		currentStateFile = Paths.get(folder.newFile("currentState.bin").toURI());
		nextStateFile = Paths.get(folder.newFile("nextState.bin").toURI());
		executorService = Executors.newFixedThreadPool(4);
		setUpFileStorage();
	}

	private void setUpFileStorage() {
		fileStorage = new DataStorageFile<>(eventloop, currentStateFile, nextStateFile, executorService,
				bufferSize, serializer, keyFunction, peers, mergeReducer, alwaysTrue);
	}

	private <T> List<T> toList(StreamProducer<T> producer) {
		final StreamConsumers.ToList<T> streamToList = StreamConsumers.toList(eventloop);
		producer.streamTo(streamToList);
		eventloop.run();
		return streamToList.getList();
	}

	private void writeStateToFile(Path currentStateFile, StreamProducer<KeyValue<Integer, Set<String>>> initStateProducer) throws IOException {
		final StreamBinarySerializer<KeyValue<Integer, Set<String>>> streamSerializer = StreamBinarySerializer.create(eventloop, serializer);
		final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, executorService, currentStateFile);
		initStateProducer.streamTo(streamSerializer.getInput());
		streamSerializer.getOutput().streamTo(fileStream);
		eventloop.run();
	}

	@Test
	public void testInitEmptyState() throws IOException, ExecutionException, InterruptedException {
		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStream(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(Collections.emptyList(), toList(callback.get()));
	}

	@Test
	public void testInitNonEmptyState() throws IOException, ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> data = newKeyValue(1, "a");
		writeStateToFile(currentStateFile, ofValue(eventloop, data));
		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStream(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(singletonList(data), toList(callback.get()));
	}

	@Test
	public void testGetSortedStreamPredicate() throws IOException, ExecutionException, InterruptedException {
		final List<KeyValue<Integer, Set<String>>> data = asList(newKeyValue(0, "a"), newKeyValue(1, "b"), newKeyValue(2, "c"), newKeyValue(3, "d"));
		writeStateToFile(currentStateFile, ofIterable(eventloop, data));

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStream(Predicates.in(asList(0, 3)), callback);

		eventloop.run();
		assertEquals(asList(data.get(0), data.get(3)), toList(callback.get()));
	}

	@Test
	public void testSynchronize() throws IOException, ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> dataId2 = newKeyValue(2, "a");
		writeStateToFile(currentStateFile, ofValue(eventloop, dataId2));

		final KeyValue<Integer, Set<String>> dataId1 = newKeyValue(1, "b");
		peers = singletonList(wrapHasSortedStream(ofValue(eventloop, dataId1)));

		setUpFileStorage();

		{
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
			fileStorage.getSortedStream(ALWAYS_TRUE, callback);
			eventloop.run();
			assertEquals(singletonList(dataId2), toList(callback.get()));
		}

		{
			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			fileStorage.synchronize(syncCallback);
			eventloop.run();
			syncCallback.get();
		}

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStream(ALWAYS_TRUE, callback);
		eventloop.run();
		assertEquals(asList(dataId1, dataId2), toList(callback.get()));
	}

	@Test
	public void testTruncateNextStateFile() throws IOException, ExecutionException, InterruptedException {
		writeStateToFile(nextStateFile, ofIterable(eventloop, asList(newKeyValue(0, "a"), newKeyValue(1, "b"), newKeyValue(2, "c"), newKeyValue(3, "d"))));

		for (int i = 0; i < 2; i++) {
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
			fileStorage.getSortedStream(ALWAYS_TRUE, callback);
			eventloop.run();
			assertEquals(Collections.emptyList(), toList(callback.get()));

			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			fileStorage.synchronize(syncCallback);
			eventloop.run();
			syncCallback.get();
		}
	}

}