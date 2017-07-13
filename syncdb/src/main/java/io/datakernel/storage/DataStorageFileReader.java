package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamReducers;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.storage.StreamMergeUtils.mergeStreams;
import static java.nio.file.StandardOpenOption.*;
import static java.util.Collections.singletonList;

public class DataStorageFileReader<K extends Comparable<K>, V> implements HasSortedStream<K, V> {
	private static final OpenOption[] READ_OPTIONS = new OpenOption[]{CREATE, READ};

	private final Eventloop eventloop;
	private final Path[] files;
	private final ExecutorService executorService;
	private final int bufferSize;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;
	private final Function<KeyValue<K, V>, K> keyFunction;

	private int currentStateFile;

	public DataStorageFileReader(Eventloop eventloop, Path currentStateFile, Path nextStateFile,
	                             ExecutorService executorService, int bufferSize,
	                             BufferSerializer<KeyValue<K, V>> bufferSerializer,
	                             Function<KeyValue<K, V>, K> keyFunction) {
		this.eventloop = eventloop;
		this.files = new Path[]{currentStateFile, nextStateFile};
		this.executorService = executorService;
		this.bufferSize = bufferSize;
		this.bufferSerializer = bufferSerializer;
		this.keyFunction = keyFunction;
	}

	@Override
	public void getSortedStream(final Predicate<K> predicate, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		AsyncFile.open(eventloop, executorService, files[currentStateFile], READ_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile asyncFile) {
				final StreamFileReader fileStream = StreamFileReader.readFileFully(eventloop, asyncFile, bufferSize);
				final StreamBinaryDeserializer<KeyValue<K, V>> deserializer = StreamBinaryDeserializer.create(eventloop, bufferSerializer);
				final StreamKeyFilter<K, KeyValue<K, V>> filter = new StreamKeyFilter<>(eventloop, predicate, keyFunction);

				fileStream.streamTo(deserializer.getInput());
				deserializer.getOutput().streamTo(filter.getInput());
				callback.setResult(filter.getOutput());
			}
		});
	}

	public void changeFiles() {
		currentStateFile = 1 - currentStateFile;
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
