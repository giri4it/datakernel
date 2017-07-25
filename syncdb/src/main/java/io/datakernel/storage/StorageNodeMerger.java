package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncCallables;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import io.datakernel.stream.processor.StreamSplitter;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.storage.StreamMergeUtils.mergeStreams;

public class StorageNodeMerger<K extends Comparable<K>, V> implements StorageNode<K, V> {

	private final Eventloop eventloop;
	private final Ordering<K> ordering = Ordering.natural();

	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, ?> reducer;
	private final List<? extends StorageNode<K, V>> peers;

	public StorageNodeMerger(Eventloop eventloop, Reducer<K, KeyValue<K, V>, KeyValue<K, V>, ?> reducer,
	                         List<? extends StorageNode<K, V>> peers) {
		this.eventloop = eventloop;
		this.reducer = reducer;
		this.peers = peers;
	}

	@Override
	public void getSortedOutput(final Predicate<K> filter, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();
		mergeStreams(eventloop, ordering, reducer, peers, filter, callback);
	}

	// TODO: add tests and predicates from each peer, here???
	@Override
	public void getSortedInput(final ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		final List<AsyncCallable<StreamConsumer<KeyValue<K, V>>>> asyncCallables = createAsyncConsumers();
		AsyncCallables.callAll(eventloop, asyncCallables).call(new ForwardingResultCallback<List<StreamConsumer<KeyValue<K, V>>>>(callback) {
			@Override
			protected void onResult(List<StreamConsumer<KeyValue<K, V>>> consumers) {
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				for (StreamConsumer<KeyValue<K, V>> consumer : consumers) {
					splitter.newOutput().streamTo(consumer);
				}
				callback.setResult(splitter.getInput());
			}
		});
	}

	private List<AsyncCallable<StreamConsumer<KeyValue<K, V>>>> createAsyncConsumers() {
		final List<AsyncCallable<StreamConsumer<KeyValue<K, V>>>> asyncCallables = new ArrayList<>();
		for (final StorageNode<K, V> peer : peers) {
			asyncCallables.add(new AsyncCallable<StreamConsumer<KeyValue<K, V>>>() {
				@Override
				public void call(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
					peer.getSortedInput(callback);
				}
			});
		}
		return asyncCallables;
	}
}
