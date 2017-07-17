package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.storage.StorageNodeFile.FILES_EXT;
import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class DataStorageFileWriterTest {
	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
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
	private Path storagePath;
	private ExecutorService executorService;
	private StorageNodeFile<Integer, Set<String>, Void> fileStorage;
	private Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, Void> reducer;

	private BufferSerializer<KeyValue<Integer, Set<String>>> serializer;

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	@Before
	public void before() throws IOException {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		serializer = SERIALIZER;
		storagePath = Paths.get(folder.newFolder().getAbsolutePath());
		executorService = Executors.newFixedThreadPool(4);
		reducer = StreamReducers.mergeSortReducer();
		setUpFileStorage();
	}

	private void setUpFileStorage() throws IOException {
		fileStorage = new StorageNodeFile<>(eventloop, storagePath, executorService, 100, serializer, reducer);
	}

	private void writeStateToFile(Path currentStateFile, StreamProducer<KeyValue<Integer, Set<String>>> initStateProducer) throws IOException {
		final StreamBinarySerializer<KeyValue<Integer, Set<String>>> streamSerializer = StreamBinarySerializer.create(eventloop, serializer);
		final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, executorService, currentStateFile);
		initStateProducer.streamTo(streamSerializer.getInput());
		streamSerializer.getOutput().streamTo(fileStream);
		eventloop.run();
	}

	private <T> List<T> toList(StreamProducer<T> producer) {
		final StreamConsumers.ToList<T> streamToList = StreamConsumers.toList(eventloop);
		producer.streamTo(streamToList);
		eventloop.run();
		return streamToList.getList();
	}

	@Test
	public void testInitEmptyState() throws IOException, ExecutionException, InterruptedException {
		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStreamProducer(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(Collections.emptyList(), toList(callback.get()));
	}

	@Test
	public void testInitNonEmptyState() throws IOException, ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> data = newKeyValue(1, "a");
		writeStateToFile(storagePath.resolve(Paths.get("1" + FILES_EXT)), ofValue(eventloop, data));
		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStreamProducer(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(singletonList(data), toList(callback.get()));
	}

	@Test
	public void testGetSortedStreamPredicate() throws IOException, ExecutionException, InterruptedException {
		final List<KeyValue<Integer, Set<String>>> data = asList(newKeyValue(0, "a"), newKeyValue(1, "b"), newKeyValue(2, "c"), newKeyValue(3, "d"));
		writeStateToFile(storagePath.resolve(Paths.get("1" + FILES_EXT)), ofIterable(eventloop, data));

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStreamProducer(Predicates.in(asList(0, 3)), callback);

		eventloop.run();
		assertEquals(asList(data.get(0), data.get(3)), toList(callback.get()));
	}

	@Test
	public void testSynchronize() throws IOException, ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> dataId1 = newKeyValue(1, "b");

		final CompletionCallbackFuture completionCallback = CompletionCallbackFuture.create();
		fileStorage.getSortedStreamConsumer(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(completionCallback) {
			@Override
			protected void onResult(StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
				ofValue(eventloop, dataId1).streamTo(listenableConsumer(consumer, completionCallback));
			}
		});
		eventloop.run();
		completionCallback.get();

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorage.getSortedStreamProducer(ALWAYS_TRUE, callback);
		eventloop.run();
		assertEquals(singletonList(dataId1), toList(callback.get()));
	}

}