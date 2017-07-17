package io.datakernel.storage;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamConsumer;
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

import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class DataStorageFileWriterTest {
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

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	@Before
	public void before() throws IOException {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		serializer = SERIALIZER;
		currentStateFile = Paths.get(folder.newFile("currentState.bin").toURI());
		nextStateFile = Paths.get(folder.newFile("nextState.bin").toURI());
		executorService = Executors.newFixedThreadPool(4);
		setUpFileStorage();
	}

	private void setUpFileStorage() {
		fileStorageWriter = new DataStorageFileWriter<>(eventloop, nextStateFile, currentStateFile, executorService, serializer);
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

		setUpFileStorage();

		final CompletionCallbackFuture completionCallback = CompletionCallbackFuture.create();
		fileStorageWriter.getSortedStreamConsumer(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(completionCallback) {
			@Override
			protected void onResult(StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
				ofValue(eventloop, dataId1).streamTo(listenableConsumer(consumer, completionCallback));
			}
		});
		eventloop.run();
		completionCallback.get();

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

			final CompletionCallbackFuture completionCallback = CompletionCallbackFuture.create();
			fileStorageWriter.getSortedStreamConsumer(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(completionCallback) {
				@Override
				protected void onResult(StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
					StreamProducers.<KeyValue<Integer, Set<String>>>closing(eventloop).streamTo(listenableConsumer(consumer, completionCallback));
				}
			});
			eventloop.run();
			completionCallback.get();
		}
	}

}