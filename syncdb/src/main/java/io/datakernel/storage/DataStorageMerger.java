package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.List;

import static io.datakernel.storage.StreamMergeUtils.mergeStreams;

public class DataStorageMerger<K extends Comparable<K>, V, A> implements HasSortedStream<K, V> {

	private final Eventloop eventloop;
	private final Ordering<K> ordering = Ordering.natural();
	private final Function<KeyValue<K, V>, K> toKey = new Function<KeyValue<K, V>, K>() {
		@Override
		public K apply(KeyValue<K, V> input) {
			return input.getKey();
		}
	};

	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;
	private final List<? extends HasSortedStream<K, V>> peers;

	public DataStorageMerger(Eventloop eventloop, Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
	                         List<? extends HasSortedStream<K, V>> peers) {
		this.eventloop = eventloop;
		this.reducer = reducer;
		this.peers = peers;
	}

	@Override
	public void getSortedStream(final Predicate<K> filter, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();
		mergeStreams(eventloop, ordering, toKey, reducer, peers, filter, callback);
	}

}
