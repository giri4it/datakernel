package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

import static io.datakernel.storage.StreamMergeUtils.mergeStreams;

public class StorageNodeMerger<K extends Comparable<K>, V, A> implements StorageNode<K, V> {

	private final Eventloop eventloop;
	private final Ordering<K> ordering = Ordering.natural();

	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;
	private final List<? extends StorageNode<K, V>> peers;

	public StorageNodeMerger(Eventloop eventloop, Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
	                         List<? extends StorageNode<K, V>> peers) {
		this.eventloop = eventloop;
		this.reducer = reducer;
		this.peers = peers;
	}

	@Override
	public void getSortedStreamProducer(final Predicate<K> filter, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();
		mergeStreams(eventloop, ordering, reducer, peers, filter, callback);
	}

}
