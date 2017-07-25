package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Ordering;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
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
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.*;

public class StorageNodeFile<K extends Comparable<K>, V, A> implements StorageNode<K, V> {
	public static final String FILES_EXT = ".log";

	private static final OpenOption[] READ_OPTIONS = new OpenOption[]{CREATE, READ};
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING};
	private static final String STORAGE_FILES_TEMPLATE = "*" + FILES_EXT;

	private final Eventloop eventloop;
	private final Path storagePath;
	private final ExecutorService executorService;
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

	public StorageNodeFile(Eventloop eventloop, Path storagePath, ExecutorService executorService, int bufferSize,
	                       BufferSerializer<KeyValue<K, V>> bufferSerializer, Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer) {
		this.eventloop = eventloop;
		this.storagePath = storagePath;
		this.executorService = executorService;
		this.bufferSize = bufferSize;
		this.bufferSerializer = bufferSerializer;
		this.reducer = reducer;
	}

	private List<AsyncCallable<AsyncFile>> createCallableAsyncFiles(DirectoryStream<Path> paths) {
		final List<AsyncCallable<AsyncFile>> asyncCallables = new ArrayList<>();
		for (final Path path : paths) {
			asyncCallables.add(new AsyncCallable<AsyncFile>() {
				@Override
				public void call(ResultCallback<AsyncFile> callback) {
					AsyncFile.open(eventloop, executorService, path, READ_OPTIONS, callback);
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
		AsyncFile.open(eventloop, executorService, nextFile, WRITE_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile asyncFile) {
				final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
				final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, asyncFile);

				serializer.getOutput().streamTo(fileStream);
				callback.setResult(serializer.getInput());
			}
		});
	}

	// refactor StreamFilter like this
	private static class StreamKeyFilter<K, V> extends AbstractStreamTransformer_1_1<V, V> {
		private final InputConsumer inputConsumer;
		private final OutputProducer outputProducer;

		protected StreamKeyFilter(Eventloop eventloop, Predicate<K> filter, Function<V, K> function) {
			super(eventloop);
			this.inputConsumer = new InputConsumer();
			this.outputProducer = new OutputProducer(filter, function);
		}

		protected final class InputConsumer extends AbstractInputConsumer {

			@Override
			protected void onUpstreamEndOfStream() {
				outputProducer.sendEndOfStream();
			}

			@Override
			public StreamDataReceiver<V> getDataReceiver() {
				return outputProducer.filter == Predicates.<K>alwaysTrue()
						? outputProducer.getDownstreamDataReceiver() : outputProducer;
			}
		}

		protected final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<V> {
			private final Predicate<K> filter;
			private final Function<V, K> function;

			private OutputProducer(Predicate<K> filter, Function<V, K> function) {
				this.filter = filter;
				this.function = function;
			}

			@Override
			protected void onDownstreamSuspended() {
				inputConsumer.suspend();
			}

			@Override
			protected void onDownstreamResumed() {
				inputConsumer.resume();
			}

			@Override
			public void onData(V item) {
				if (filter.apply(function.apply(item))) {
					send(item);
				}
			}
		}

		@Override
		protected AbstractInputConsumer getInputImpl() {
			return inputConsumer;
		}

		@Override
		protected AbstractOutputProducer getOutputImpl() {
			return outputProducer;
		}
	}
}
