package io.datakernel.storage;

import com.google.common.base.Predicate;
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

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.*;

public class DataStorageFileWriter<K extends Comparable<K>, V> implements Synchronizer {
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING};

	private final Eventloop eventloop;
	private final Path[] files;
	private final ExecutorService executorService;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;
	private final Predicate<K> filter;
	private final HasSortedStreamProducer<K, V> peer;

	private int currentWriteFile;

	public DataStorageFileWriter(Eventloop eventloop, Path currentWriteFile, Path nextWriteFile,
	                             ExecutorService executorService, BufferSerializer<KeyValue<K, V>> bufferSerializer,
	                             HasSortedStreamProducer<K, V> peer, Predicate<K> filter) {
		this.eventloop = eventloop;
		this.files = new Path[]{currentWriteFile, nextWriteFile};
		this.executorService = executorService;
		this.bufferSerializer = bufferSerializer;
		this.peer = peer;
		this.filter = filter;
	}

	@Override
	public void synchronize(final CompletionCallback callback) {
		peer.getSortedStreamProducer(filter, new ForwardingResultCallback<StreamProducer<KeyValue<K, V>>>(callback) {
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
