package io.datakernel.storage;

import com.google.common.base.Predicate;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ListenableResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerDecorator;
import io.datakernel.stream.processor.StreamSplitter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;

public final class StorageNodeListenable<K, V> implements StorageNode<K, V> {
	private final Eventloop eventloop;
	private final StorageNode<K, V> hasSortedStreamProducer;

	private ListenableResultCallback<StreamProducer<KeyValue<K, V>>> listenable = null;

	public StorageNodeListenable(Eventloop eventloop, StorageNode<K, V> hasSortedStreamProducer) {
		this.eventloop = eventloop;
		this.hasSortedStreamProducer = hasSortedStreamProducer;
	}

	@Override
	public void getSortedStreamProducer(final Predicate<K> predicate, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
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
		hasSortedStreamProducer.getSortedStreamProducer(predicate, new ResultCallback<StreamProducer<KeyValue<K, V>>>() {
			@Override
			protected void onResult(StreamProducer<KeyValue<K, V>> producer) {
				final ListenableResultCallback<StreamProducer<KeyValue<K, V>>> callbacks = listenable;
				listenable = ListenableResultCallback.create();
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				producer.streamTo(listenableConsumer(splitter.getInput(), processNextListeners(predicate)));

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
				listenable = ListenableResultCallback.create();
				callbacks.setException(e);
			}
		});
	}

	private CompletionCallback processNextListeners(final Predicate<K> predicate) {
		return new CompletionCallback() {
			@Override
			protected void onComplete() {
				onCompleteOrException();
			}

			@Override
			protected void onException(Exception e) {
				onCompleteOrException();
			}

			private void onCompleteOrException() {
				if (listenable.size() != 0) {
					eventloop.post(new Runnable() {
						@Override
						public void run() {
							doGetSortedStream(predicate);
						}
					});
				} else {
					listenable = null;
				}
			}
		};
	}
}
