package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinarySerializer;
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

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class DataStorageFileReaderTest {
	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
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
	private DataStorageFileReader<Integer, Set<String>> fileStorageReader;

	private BufferSerializer<KeyValue<Integer, Set<String>>> serializer = SERIALIZER;
	private Function<KeyValue<Integer, Set<String>>, Integer> keyFunction = KEY_FUNCTION;
	private int bufferSize = 1;

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	@Before
	public void before() throws IOException {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		currentStateFile = Paths.get(folder.newFile("currentState.bin").toURI());
		nextStateFile = Paths.get(folder.newFile("nextState.bin").toURI());
		executorService = Executors.newFixedThreadPool(4);
		setUpFileStorage();
	}

	private void setUpFileStorage() {
		fileStorageReader = new DataStorageFileReader<>(eventloop, currentStateFile, nextStateFile, executorService,
				bufferSize, serializer, keyFunction);
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
		fileStorageReader.getSortedStreamProducer(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(Collections.emptyList(), toList(callback.get()));
	}

	@Test
	public void testInitNonEmptyState() throws IOException, ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> data = newKeyValue(1, "a");
		writeStateToFile(currentStateFile, ofValue(eventloop, data));
		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorageReader.getSortedStreamProducer(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(singletonList(data), toList(callback.get()));
	}

	@Test
	public void testGetSortedStreamPredicate() throws IOException, ExecutionException, InterruptedException {
		final List<KeyValue<Integer, Set<String>>> data = asList(newKeyValue(0, "a"), newKeyValue(1, "b"), newKeyValue(2, "c"), newKeyValue(3, "d"));
		writeStateToFile(currentStateFile, ofIterable(eventloop, data));

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		fileStorageReader.getSortedStreamProducer(Predicates.in(asList(0, 3)), callback);

		eventloop.run();
		assertEquals(asList(data.get(0), data.get(3)), toList(callback.get()));
	}

	@Test
	public void testChangeFiles() throws IOException, ExecutionException, InterruptedException {
		final List<KeyValue<Integer, Set<String>>> data1 = asList(newKeyValue(0, "a"), newKeyValue(1, "b"));
		final List<KeyValue<Integer, Set<String>>> data2 = asList(newKeyValue(10, "aa"), newKeyValue(11, "bb"));
		writeStateToFile(currentStateFile, ofIterable(eventloop, data1));
		writeStateToFile(nextStateFile, ofIterable(eventloop, data2));

		for (List<KeyValue<Integer, Set<String>>> data : asList(data1, data2, data1, data2)) {
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
			fileStorageReader.getSortedStreamProducer(ALWAYS_TRUE, callback);
			eventloop.run();
			assertEquals(data, toList(callback.get()));

			fileStorageReader.changeFiles();
		}
	}

}