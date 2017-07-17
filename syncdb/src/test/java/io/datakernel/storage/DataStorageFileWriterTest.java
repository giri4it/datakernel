package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;
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
	private Path currentStateFile;
	private Path nextStateFile;
	private ExecutorService executorService;
	private DataStorageFileWriter<Integer, Set<String>> fileStorageWriter;

	private BufferSerializer<KeyValue<Integer, Set<String>>> serializer;
	private HasSortedStreamProducer<Integer, Set<String>> peer;

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	private static HasSortedStreamProducer<Integer, Set<String>> wrapHasSortedStream(final StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		return new HasSortedStreamProducer<Integer, Set<String>>() {
			@Override
			public void getSortedStreamProducer(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				callback.setResult(producer);
			}
		};
	}

	@Before
	public void before() throws IOException {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		peer = wrapHasSortedStream(StreamProducers.<KeyValue<Integer, Set<String>>>closing(eventloop));
		serializer = SERIALIZER;
		currentStateFile = Paths.get(folder.newFile("currentState.bin").toURI());
		nextStateFile = Paths.get(folder.newFile("nextState.bin").toURI());
		executorService = Executors.newFixedThreadPool(4);
		setUpFileStorage();
	}

	private void setUpFileStorage() {
		fileStorageWriter = new DataStorageFileWriter<>(eventloop, nextStateFile, currentStateFile, executorService,
				serializer, peer, ALWAYS_TRUE);
	}

	private void writeStateToFile(Path currentStateFile, StreamProducer<KeyValue<Integer, Set<String>>> initStateProducer) throws IOException {
		final StreamBinarySerializer<KeyValue<Integer, Set<String>>> streamSerializer = StreamBinarySerializer.create(eventloop, serializer);
		final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, executorService, currentStateFile);
		initStateProducer.streamTo(streamSerializer.getInput());
		streamSerializer.getOutput().streamTo(fileStream);
		eventloop.run();
	}

	private void readStateFromFile(Path file, final ResultCallback<List<KeyValue<Integer, Set<String>>>> callback) {
		AsyncFile.open(eventloop, executorService, file, new OpenOption[]{StandardOpenOption.READ}, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile result) {
				final StreamFileReader fileReader = StreamFileReader.readFileFully(eventloop, result, 100);
				final StreamBinaryDeserializer<KeyValue<Integer, Set<String>>> deserializer = StreamBinaryDeserializer.create(eventloop, serializer);
				final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> consumerToList = StreamConsumers.toList(eventloop);

				fileReader.streamTo(deserializer.getInput());
				deserializer.getOutput().streamTo(consumerToList);
				consumerToList.setCompletionCallback(new ForwardingCompletionCallback(callback) {
					@Override
					protected void onComplete() {
						callback.setResult(consumerToList.getList());
					}
				});

			}
		});
	}

	@Test
	public void testSynchronize() throws IOException, ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> dataId1 = newKeyValue(1, "b");
		peer = wrapHasSortedStream(ofValue(eventloop, dataId1));

		setUpFileStorage();

		final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
		fileStorageWriter.synchronize(syncCallback);
		eventloop.run();
		syncCallback.get();

		final ResultCallbackFuture<List<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		readStateFromFile(nextStateFile, callback);
		eventloop.run();
		assertEquals(Collections.singletonList(dataId1), callback.get());
	}

	@Test
	public void testTruncateNextStateFile() throws IOException, ExecutionException, InterruptedException {
		writeStateToFile(nextStateFile, ofIterable(eventloop, asList(newKeyValue(0, "a"), newKeyValue(1, "b"), newKeyValue(2, "c"), newKeyValue(3, "d"))));

		final Iterator<Path> files = Iterables.cycle(asList(currentStateFile, nextStateFile)).iterator();
		for (int i = 0; i < 2; i++) {
			final ResultCallbackFuture<List<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
			readStateFromFile(files.next(), callback);
			eventloop.run();
			assertEquals(Collections.emptyList(), callback.get());

			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			fileStorageWriter.synchronize(syncCallback);
			eventloop.run();
			syncCallback.get();
		}
	}

}