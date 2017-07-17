package io.datakernel.storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.merger.Merger;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;
import static io.datakernel.stream.StreamProducers.ofIterable;

public final class StorageNodeTreeMap<K extends Comparable<K>, V> implements StorageNode<K, V> {
	private final Eventloop eventloop;
	private final Merger<KeyValue<K, V>> merger;
	private final Function<Map.Entry<K, V>, KeyValue<K, V>> toKeyValue = new Function<Map.Entry<K, V>, KeyValue<K, V>>() {
		@Override
		public KeyValue<K, V> apply(Map.Entry<K, V> input) {
			return new KeyValue<>(input.getKey(), input.getValue());
		}
	};

	private final TreeMap<K, V> values;

	public StorageNodeTreeMap(Eventloop eventloop, TreeMap<K, V> values, Merger<KeyValue<K, V>> merger) {
		this.eventloop = eventloop;
		this.merger = merger;
		this.values = values;
	}

	@Override
	public void getSortedStreamProducer(Predicate<K> predicate, ResultCallback<StreamProducer<KeyValue<K, V>>> callback) {
		assert eventloop.inEventloopThread();
		callback.setResult(ofIterable(eventloop, transform(filterKeys(values, predicate).entrySet(), toKeyValue)));
	}

	@Override
	public void getSortedStreamConsumer(ResultCallback<StreamConsumer<KeyValue<K, V>>> callback) {
		callback.setResult(new AbstractStreamConsumer<KeyValue<K, V>>(eventloop) {
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
		return values.put(key, value);
	}

	public int size() {
		assert eventloop.inEventloopThread();
		return values.size();
	}
}
