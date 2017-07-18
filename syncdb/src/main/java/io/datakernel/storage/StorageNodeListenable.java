package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamSplitter;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;

public final class StorageNodeListenable<K extends Comparable<K>, V, A> implements StorageNode<K, V> {
	private final Eventloop eventloop;
	private final StorageNode<K, V> storageNode;

	private StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;
	private Function<KeyValue<K, V>, K> keyFunction = new Function<KeyValue<K, V>, K>() {
		@Override
		public K apply(KeyValue<K, V> input) {
			return input.getKey();
		}
	};

	// null -> no requests, not null -> process one request
	private List<ResultCallback<StreamProducer<KeyValue<K, V>>>> producerCallbacks = null;
	private List<ResultCallback<StreamConsumer<KeyValue<K, V>>>> consumerCallbacks = null;

	// asserts only,  self check in runtime, improve readability
	private int getSortedProducerCounter;
	private int getSortedConsumerCounter;

	public StorageNodeListenable(Eventloop eventloop, StorageNode<K, V> storageNode,
	                             StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer) {
		this.eventloop = eventloop;
		this.storageNode = storageNode;
		this.reducer = reducer;
	}

	@Override
	public void getSortedStreamProducer(final Predicate<K> predicate, final ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();

		if (producerCallbacks != null) {
			producerCallbacks.add(callback);
			return;
		}

		producerCallbacks = new ArrayList<>();
		producerCallbacks.add(callback);
		eventloop.post(doGetSortedStreamProducerRunnable(predicate));
	}

	private void doGetSortedStreamProducer(final Predicate<K> predicate) {
		//noinspection AssertWithSideEffects
		assert ++getSortedProducerCounter == 1;

		storageNode.getSortedStreamProducer(predicate, new ResultCallback<StreamProducer<KeyValue<K, V>>>() {
			@Override
			protected void onResult(StreamProducer<KeyValue<K, V>> producer) {
				final List<ResultCallback<StreamProducer<KeyValue<K, V>>>> callbacks = producerCallbacks;
				producerCallbacks = new ArrayList<>();
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				producer.streamTo(listenableConsumer(splitter.getInput(), processNextProducerCallbacks(predicate)));

				for (ResultCallback<StreamProducer<KeyValue<K, V>>> callback : callbacks) {
					callback.setResult(splitter.newOutput());
				}
			}

			@Override
			protected void onException(Exception e) {
				final List<ResultCallback<StreamProducer<KeyValue<K, V>>>> callbacks = producerCallbacks;
				producerCallbacks = new ArrayList<>();
				for (ResultCallback<StreamProducer<KeyValue<K, V>>> callback : callbacks) callback.setException(e);
			}
		});
	}

	private Runnable doGetSortedStreamProducerRunnable(final Predicate<K> predicate) {
		return new Runnable() {
			@Override
			public void run() {
				doGetSortedStreamProducer(predicate);
			}
		};
	}

	@Override
	public void getSortedStreamConsumer(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();

		if (consumerCallbacks != null) {
			consumerCallbacks.add(callback);
			return;
		}

		consumerCallbacks = new ArrayList<>();
		consumerCallbacks.add(callback);
		eventloop.post(doGetSortedStreamConsumerRunnable());
	}

	private void doGetSortedStreamConsumer() {
		//noinspection AssertWithSideEffects
		assert ++getSortedConsumerCounter == 1;

		storageNode.getSortedStreamConsumer(new ResultCallback<StreamConsumer<KeyValue<K, V>>>() {
			@Override
			protected void onResult(StreamConsumer<KeyValue<K, V>> consumer) {
				final List<ResultCallback<StreamConsumer<KeyValue<K, V>>>> callbacks = consumerCallbacks;
				consumerCallbacks = new ArrayList<>();

				final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, Ordering.<K>natural());
				streamReducer.getOutput().streamTo(listenableConsumer(consumer, processNextConsumerCallbacks()));

				for (ResultCallback<StreamConsumer<KeyValue<K, V>>> callback : callbacks) {
					callback.setResult(streamReducer.newInput(keyFunction, reducer));
				}
			}

			@Override
			protected void onException(Exception e) {
				final List<ResultCallback<StreamConsumer<KeyValue<K, V>>>> callbacks = consumerCallbacks;
				consumerCallbacks = new ArrayList<>();
				for (ResultCallback<StreamConsumer<KeyValue<K, V>>> callback : callbacks) callback.setException(e);
			}
		});
	}

	private Runnable doGetSortedStreamConsumerRunnable() {
		return new Runnable() {
			@Override
			public void run() {
				doGetSortedStreamConsumer();
			}
		};
	}

	private CompletionCallback processNextProducerCallbacks(final Predicate<K> predicate) {
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
				//noinspection AssertWithSideEffects
				assert --getSortedProducerCounter == 0;

				if (producerCallbacks.size() != 0) {
					eventloop.post(doGetSortedStreamProducerRunnable(predicate));
				} else {
					producerCallbacks = null;
				}
			}
		};
	}

	private CompletionCallback processNextConsumerCallbacks() {
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
				//noinspection AssertWithSideEffects
				assert --getSortedConsumerCounter == 0;

				if (consumerCallbacks.size() != 0) {
					eventloop.post(doGetSortedStreamConsumerRunnable());
				} else {
					consumerCallbacks = null;
				}
			}
		};
	}
}
