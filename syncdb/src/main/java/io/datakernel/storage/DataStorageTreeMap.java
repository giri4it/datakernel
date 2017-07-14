package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.merger.Merger;
import io.datakernel.merger.MergerReducer;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;
import static io.datakernel.storage.StreamMergeUtils.mergeStreams;
import static io.datakernel.stream.StreamProducers.ofIterable;

// interface DataStorage extends HasSortedStream<Integer, Set<String>>, Synchronizer ???
@SuppressWarnings("WeakerAccess")
public final class DataStorageTreeMap<K extends Comparable<K>, V, A> implements HasSortedStream<K, V>, Synchronizer {
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

	private final Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;
	private final Merger<KeyValue<K, V>> merger;
	private final List<? extends HasSortedStream<K, V>> peers;
	private final Predicate<K> keyFilter;

	private final TreeMap<K, V> values;

	public DataStorageTreeMap(Eventloop eventloop, TreeMap<K, V> values, List<? extends HasSortedStream<K, V>> peers,
	                          Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer, Predicate<K> keyFilter) {
		this.eventloop = eventloop;
		// as argument ???
		this.merger = new MergerReducer<>(reducer);
		this.values = values;
		this.reducer = reducer;
		this.peers = peers;
		this.keyFilter = keyFilter;
	}

	@Override
	public void getSortedStream(Predicate<K> predicate, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();
		callback.setResult(ofIterable(eventloop, transform(filterKeys(values, predicate).entrySet(), TO_KEY_VALUE)));
	}

	@Override
	public void synchronize(final CompletionCallback callback) {
		assert eventloop.inEventloopThread();
		mergeStreams(eventloop, ordering, toKey, reducer, peers, keyFilter, new ForwardingResultCallback<StreamProducer<KeyValue<K, V>>>(callback) {
			@Override
			protected void onResult(StreamProducer<KeyValue<K, V>> producer) {
				producer.streamTo(new AbstractStreamConsumer<KeyValue<K, V>>(eventloop) {
					@Override
					public StreamDataReceiver<KeyValue<K, V>> getDataReceiver() {
						return new StreamDataReceiver<KeyValue<K, V>>() {
							@Override
							public void onData(KeyValue<K, V> newValue) {
								final K key = newValue.getKey();
								final KeyValue<K, V> oldValue = new KeyValue<>(key, values.get(key));
								values.put(key, merger.merge(newValue, oldValue).getValue());
							}
						};
					}

					@Override
					protected void onEndOfStream() {
						callback.setComplete();
					}
				});
			}
		});
	}

	public boolean hasKey(K key) {
		assert eventloop.inEventloopThread();
		return values.containsKey(key);
	}

	public V get(K key) {
		assert eventloop.inEventloopThread();
		return values.get(key);
	}

	public V put(K key, V value) {
		assert eventloop.inEventloopThread();
		assert !values.containsKey(key);
		return values.put(key, value);
	}

	public int size() {
		assert eventloop.inEventloopThread();
		return values.size();
	}
}
