package io.datakernel.storage;

import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.StreamBinarySerializer;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static java.nio.file.StandardOpenOption.*;

public class DataStorageFileWriter<K extends Comparable<K>, V> implements HasSortedStreamConsumer<K, V> {
	private static final OpenOption[] WRITE_OPTIONS = new OpenOption[]{CREATE, WRITE, TRUNCATE_EXISTING};

	private final Eventloop eventloop;
	private final Path[] files;
	private final ExecutorService executorService;
	private final BufferSerializer<KeyValue<K, V>> bufferSerializer;

	// при перезапуску ми маємо почати читати з того на якому закінчили
	private int currentWriteFile;

	public DataStorageFileWriter(Eventloop eventloop, Path currentWriteFile, Path nextWriteFile,
	                             ExecutorService executorService, BufferSerializer<KeyValue<K, V>> bufferSerializer) {
		this.eventloop = eventloop;
		this.files = new Path[]{currentWriteFile, nextWriteFile};
		this.executorService = executorService;
		this.bufferSerializer = bufferSerializer;
	}

	@Override
	public void getSortedStreamConsumer(final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		AsyncFile.open(eventloop, executorService, files[currentWriteFile], WRITE_OPTIONS, new ForwardingResultCallback<AsyncFile>(callback) {
			@Override
			protected void onResult(AsyncFile asyncFile) {
				final StreamBinarySerializer<KeyValue<K, V>> serializer = StreamBinarySerializer.create(eventloop, bufferSerializer);
				final StreamFileWriter fileStream = StreamFileWriter.create(eventloop, asyncFile);
				fileStream.setFlushCallback(new ForwardingCompletionCallback(callback) {
					@Override
					protected void onComplete() {
						currentWriteFile = 1 - currentWriteFile;
					}
				});

				serializer.getOutput().streamTo(fileStream);
				callback.setResult(serializer.getInput());
			}
		});
	}
}
