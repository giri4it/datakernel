package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Ordering;
import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.RunnableWithException;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.streams.StreamKeyFilter;
import io.datakernel.stream.StreamConsumer;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Lists.newArrayList;
import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static java.nio.file.StandardOpenOption.*;

public class StorageNodeFile<K extends Comparable<K>, V, A> implements StorageNode<K, V>, Consolidator {
	public static final String FILES_EXT = ".log";
	public static final String STORAGE_FILES_TEMPLATE = "*" + FILES_EXT;
	public static final Comparator<Path> FILE_COMPARATOR = new Comparator<Path>() {
		@Override
		public int compare(Path o1, Path o2) {
			return Long.compare(filePathToTimestamp(o1), filePathToTimestamp(o2));
		}
	};


	private static final OpenOption[] READ_OPTIONS = new OpenOption[]{CREATE, READ};
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING};

	private final Eventloop eventloop;
	private final Path storagePath;
	private final ExecutorService executor;
	private final int bufferSize;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;
	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;
	private final Ordering<K> ordering = Ordering.natural();
	private final Function<KeyValue<K, V>, K> keyFunction = new Function<KeyValue<K, V>, K>() {
		@Override
		public K apply(KeyValue<K, V> input) {
			return input.getKey();
		}
	};

	public StorageNodeFile(Eventloop eventloop, Path storagePath, ExecutorService executor, int bufferSize,
	                       BufferSerializer<KeyValue<K, V>> bufferSerializer, Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer) {
		this.eventloop = eventloop;
		this.storagePath = storagePath;
		this.executor = executor;
		this.bufferSize = bufferSize;
		this.bufferSerializer = bufferSerializer;
		this.reducer = reducer;
	}

	private static long filePathToTimestamp(Path filePath) {
		final String fileName = filePath.getFileName().toString();
		return Long.valueOf(fileName.substring(0, fileName.indexOf('.')));
	}

	private List<AsyncCallable<AsyncFile>> createCallableAsyncFiles(DirectoryStream<Path> paths) {
		final List<AsyncCallable<AsyncFile>> asyncCallables = new ArrayList<>();
		for (final Path path : paths) {
			asyncCallables.add(new AsyncCallable<AsyncFile>() {
				@Override
				public void call(ResultCallback<AsyncFile> callback) {
					AsyncFile.open(eventloop, executor, path, READ_OPTIONS, callback);
				}
			});
		}
		return asyncCallables;
	}

	@Override
	public void getSortedOutput(final Predicate<K> predicate, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();
		try (final DirectoryStream<Path> paths = Files.newDirectoryStream(storagePath, STORAGE_FILES_TEMPLATE)) {
			final List<AsyncCallable<AsyncFile>> asyncCallables = createCallableAsyncFiles(paths);
			AsyncCallables.callAll(eventloop, asyncCallables).call(new ForwardingResultCallback<List<AsyncFile>>(callback) {
				@Override
				protected void onResult(List<AsyncFile> files) {
					final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, ordering);
					for (AsyncFile file : files) {
						final StreamFileReader fileReader = StreamFileReader.readFileFully(eventloop, file, bufferSize);
						final StreamBinaryDeserializer<KeyValue<K, V>> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);
						final StreamKeyFilter<K, KeyValue<K, V>> filter = new StreamKeyFilter<>(eventloop, predicate, keyFunction);

						fileReader.streamTo(deserializer.getInput());
						deserializer.getOutput().streamTo(filter.getInput());
						filter.getOutput().streamTo(streamReducer.newInput(keyFunction, reducer));
					}
					callback.setResult(streamReducer.getOutput());
				}
			});
		} catch (IOException e) {
			callback.setException(e);
		}
	}

	private static String createFileName(long timestamp) {
		return Long.toString(timestamp) + FILES_EXT;
	}

	@Override
	public void getSortedInput(final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();

		final Path nextFile = storagePath.resolve(createFileName(eventloop.currentTimeMillis()));
		AsyncFile.open(eventloop, executor, nextFile, WRITE_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile asyncFile) {
				final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
				final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, asyncFile);

				serializer.getOutput().streamTo(fileStream);
				callback.setResult(serializer.getInput());
			}
		});
	}

	@Override
	public void consolidate(final CompletionCallback callback) {
		getSortedOutput(Predicates.<K>alwaysTrue(), new ForwardingResultCallback<StreamProducer<KeyValue<K, V>>>(callback) {
			@Override
			protected void onResult(final StreamProducer<KeyValue<K, V>> producer) {
				getSortedInput(new ForwardingResultCallback<StreamConsumer<KeyValue<K, V>>>(callback) {
					@Override
					protected void onResult(StreamConsumer<KeyValue<K, V>> consumer) {
						producer.streamTo(listenableConsumer(consumer, new ForwardingCompletionCallback(callback) {
							@Override
							protected void onComplete() {
								eventloop.runConcurrently(executor, new RunnableWithException() {
									@Override
									public void runWithException() throws Exception {
										removeOldFiles();
									}
								}, callback);
							}
						}));
					}
				});
			}
		});
	}

	private void removeOldFiles() throws IOException {
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(storagePath, STORAGE_FILES_TEMPLATE)) {
			final List<Path> filesSorted = newArrayList(stream);
			final Path lastCreatedFile = Collections.max(filesSorted, FILE_COMPARATOR);
			for (Path path : filesSorted) {
				if (!path.equals(lastCreatedFile)) Files.delete(path);
			}
		}
	}
}
