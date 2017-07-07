package storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;
import static java.util.Collections.singletonList;
import static storage.StreamMergeUtils.mergeStreams;

// interface DataStorage extends HasSortedStream<Integer, Set<String>>, Synchronizer ???
public class DataStorageSimple<K extends Comparable<K>, V> implements HasSortedStream<K, V>, Synchronizer {

	private final Eventloop eventloop;
	private final Ordering<K> ordering = Ordering.natural();
	private final Function<Map.Entry<K, V>, KeyValue<K, V>> TO_KEY_VALUE = new Function<Map.Entry<K, V>, KeyValue<K, V>>() {
		@Override
		public KeyValue<K, V> apply(Map.Entry<K, V> input) {
			return new KeyValue<>(input.getKey(), input.getValue());
		}
	};

	private final Function<KeyValue<K, V>, K> toKey = new Function<KeyValue<K, V>, K>() {
		@Override
		public K apply(KeyValue<K, V> input) {
			return input.getKey();
		}
	};

	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, KeyValue<K, V>> reducer;
	private final List<? extends HasSortedStream<K, V>> peers;
	private final Predicate<K> keyFilter;

	private final TreeMap<K, V> values;

	public DataStorageSimple(Eventloop eventloop, TreeMap<K, V> values, List<? extends HasSortedStream<K, V>> peers,
	                         Reducer<K, KeyValue<K, V>, KeyValue<K, V>, KeyValue<K, V>> reducer, Predicate<K> keyFilter) {
		this.eventloop = eventloop;
		this.values = values;
		this.reducer = reducer;
		this.peers = peers;
		this.keyFilter = keyFilter;
	}

	@Override
	public StreamProducer<KeyValue<K, V>> getSortedStream(Predicate<K> predicate) {
		assert eventloop.inEventloopThread();
		return StreamProducers.ofIterable(eventloop, transform(filterKeys(values, predicate).entrySet(), TO_KEY_VALUE));
	}

	@Override
	public void synchronize(final CompletionCallback callback) {
		// delete
		final Iterable<HasSortedStream<K, V>> peers = concat(this.peers, singletonList(this));
		final StreamProducer<KeyValue<K, V>> producer = mergeStreams(eventloop, ordering, toKey, reducer, peers, keyFilter);
		producer.streamTo(new AbstractStreamConsumer<KeyValue<K, V>>(eventloop) {
			@Override
			public StreamDataReceiver<KeyValue<K, V>> getDataReceiver() {
				return new StreamDataReceiver<KeyValue<K, V>>() {
					@Override
					public void onData(KeyValue<K, V> newValue) {
						// use merge here
						values.put(newValue.getKey(), newValue.getValue());
					}
				};
			}

			@Override
			protected void onEndOfStream() {
				callback.setComplete();
			}
		});
	}
}
