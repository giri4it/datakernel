package io.datakernel.storage;

import com.google.common.base.Predicate;
import io.datakernel.async.ListenableResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerDecorator;
import io.datakernel.stream.processor.StreamSplitter;

public final class DataStorageListenable<K, V> implements HasSortedStream<K, V> {
	private final Eventloop eventloop;
	private final HasSortedStream<K, V> hasSortedStream;

	private ListenableResultCallback<StreamProducer<KeyValue<K, V>>> listenable = null;

	public DataStorageListenable(Eventloop eventloop, HasSortedStream<K, V> hasSortedStream) {
		this.eventloop = eventloop;
		this.hasSortedStream = hasSortedStream;
	}

	@Override
	public void getSortedStream(final Predicate<K> predicate, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();

		if (listenable != null) {
			listenable.addListener(callback);
			return;
		}

		listenable = ListenableResultCallback.create();
		listenable.addListener(callback);
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				doGetSortedStream(predicate);
			}
		});
	}

	private void doGetSortedStream(final Predicate<K> predicate) {
		hasSortedStream.getSortedStream(predicate, new ResultCallback<StreamProducer<KeyValue<K, V>>>() {
			@Override
			protected void onResult(StreamProducer<KeyValue<K, V>> producer) {
				final ListenableResultCallback<StreamProducer<KeyValue<K, V>>> callbacks = listenable;
				listenable = null;
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				producer.streamTo(splitter.getInput());
				callbacks.setResult(new StreamProducerDecorator<KeyValue<K, V>>(producer) {
					@Override
					public void streamTo(StreamConsumer<KeyValue<K, V>> downstreamConsumer) {
						splitter.newOutput().streamTo(downstreamConsumer);
					}
				});
			}

			@Override
			protected void onException(Exception e) {
				final ListenableResultCallback<StreamProducer<KeyValue<K, V>>> callbacks = listenable;
				listenable = null;
				callbacks.setException(e);
			}
		});
	}
}
