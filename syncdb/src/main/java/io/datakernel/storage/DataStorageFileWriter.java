package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinarySerializer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.datakernel.storage.StreamMergeUtils.mergeStreams;
import static java.nio.file.StandardOpenOption.*;

public class DataStorageFileWriter<K extends Comparable<K>, V, A> implements Synchronizer {
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING};

	private final Eventloop eventloop;
	private final Path[] files;
	private final ExecutorService executorService;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;
	private final Function<KeyValue<K, V>, K> keyFunction;
	private final Predicate<K> filter;
	private final List<? extends HasSortedStreamProducer<K, V>> peers;
	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;

	private int currentWriteFile;

	public DataStorageFileWriter(Eventloop eventloop, Path currentWriteFile, Path nextWriteFile,
	                             ExecutorService executorService,
	                             BufferSerializer<KeyValue<K, V>> bufferSerializer,
	                             Function<KeyValue<K, V>, K> keyFunction,
	                             List<? extends HasSortedStreamProducer<K, V>> peers,
	                             Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
	                             Predicate<K> filter) {
		this.eventloop = eventloop;
		this.files = new Path[]{currentWriteFile, nextWriteFile};
		this.executorService = executorService;
		this.bufferSerializer = bufferSerializer;
		this.keyFunction = keyFunction;
		this.peers = peers;
		this.reducer = reducer;
		this.filter = filter;
	}

	@Override
	public void synchronize(final CompletionCallback callback) {
		mergeStreams(eventloop, Ordering.<K>natural(), keyFunction, reducer, peers, filter, new ForwardingResultCallback<StreamProducer<KeyValue<K, V>>>(callback) {
			@Override
			protected void onResult(final StreamProducer<KeyValue<K, V>> producer) {
				AsyncFile.open(eventloop, executorService, files[currentWriteFile], WRITE_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
					@Override
					protected void onResult(AsyncFile asyncFile) {
						final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
						final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, asyncFile);
						fileStream.setFlushCallback(new ForwardingCompletionCallback(callback) {
							@Override
							protected void onComplete() {
								currentWriteFile = 1 - currentWriteFile;
								callback.setComplete();
							}
						});

						producer.streamTo(serializer.getInput());
						serializer.getOutput().streamTo(fileStream);
					}
				});
			}
		});
	}
}
