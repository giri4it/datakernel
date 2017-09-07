package io.datakernel.storage;

import com.google.common.collect.Ordering;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.RunnableException;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.streams.StreamKeyFilter;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers.StreamConsumerListenable;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static java.nio.file.StandardOpenOption.*;

public class StorageNodeFile<K extends Comparable<K>, V, A> implements StorageNode<K, V>, Consolidator {
	public static final String FILES_EXT = ".log";
	public static final String STORAGE_FILES_TEMPLATE = "*" + FILES_EXT;

	private static final OpenOption[] READ_OPTIONS = new OpenOption[]{CREATE, READ};
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING};

	private final Eventloop eventloop;
	private final Path storagePath;
	private final ExecutorService executor;
	private final int bufferSize;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;
	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;
	private final Ordering<K> ordering = Ordering.natural();

	public StorageNodeFile(Eventloop eventloop, Path storagePath, ExecutorService executor, int bufferSize,
	                       BufferSerializer<KeyValue<K, V>> bufferSerializer, Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer) {
		this.eventloop = eventloop;
		this.storagePath = storagePath;
		this.executor = executor;
		this.bufferSize = bufferSize;
		this.bufferSerializer = bufferSerializer;
		this.reducer = reducer;
	}

	private List<AsyncCallable<AsyncFile>> createCallableAsyncFiles(DirectoryStream<Path> paths) {
		final List<AsyncCallable<AsyncFile>> asyncCallables = new ArrayList<>();
		for (final Path path : paths) {
			asyncCallables.add(() -> AsyncFile.openAsync(eventloop, executor, path, READ_OPTIONS));
		}
		return asyncCallables;
	}

	@Override
	public CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(Predicate<K> predicate) {
		assert eventloop.inEventloopThread();
		try (final DirectoryStream<Path> paths = Files.newDirectoryStream(storagePath, STORAGE_FILES_TEMPLATE)) {
			final List<AsyncCallable<AsyncFile>> asyncCallables = createCallableAsyncFiles(paths);
			return AsyncCallables.callAll(eventloop, asyncCallables).call().thenApply(files -> {
				final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, ordering);
				for (AsyncFile file : files) {
					final StreamFileReader fileReader = StreamFileReader.readFileFully(eventloop, file, bufferSize);
					final StreamBinaryDeserializer<KeyValue<K, V>> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);
					final StreamKeyFilter<K, KeyValue<K, V>> filter = new StreamKeyFilter<>(eventloop, predicate, KeyValue::getKey);

					fileReader.streamTo(deserializer.getInput());
					deserializer.getOutput().streamTo(filter.getInput());
					filter.getOutput().streamTo(streamReducer.newInput(KeyValue::getKey, reducer));
				}
				return streamReducer.getOutput();
			});
		} catch (IOException e) {
			return SettableStage.immediateFailedStage(e);
		}
	}

	private static String createFileName(long timestamp) {
		return Long.toString(timestamp) + FILES_EXT;
	}

	@Override
	public CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput() {
		assert eventloop.inEventloopThread();

		final Path nextFile = storagePath.resolve(createFileName(eventloop.currentTimeMillis()));
		return AsyncFile.openAsync(eventloop, executor, nextFile, WRITE_OPTIONS).thenApply(asyncFile -> {
			final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
			final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, asyncFile);

			serializer.getOutput().streamTo(fileStream);
			return serializer.getInput();
		});
	}

	@Override
	public CompletionStage<Void> consolidate() {
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(storagePath, STORAGE_FILES_TEMPLATE)) {
			final List<Path> files = newArrayList(stream);

			return getSortedOutput(k -> true).thenCompose(producer -> getSortedInput().thenAccept(consumer -> {
				final StreamConsumerListenable<KeyValue<K, V>> listenableConsumer = listenableConsumer(consumer);
				producer.streamTo(listenableConsumer);
				listenableConsumer.getStage().thenCompose(aVoid -> eventloop.runConcurrently(executor, () -> files.forEach(path -> {
					try {
						Files.delete(path);
					} catch (IOException e) {
						throw new RunnableException(e);
					}
				})));
			}));
		} catch (IOException e) {
			return SettableStage.immediateFailedStage(e);
		}
	}
}
