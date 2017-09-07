package io.datakernel.storage;

import com.google.common.collect.Maps;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.merger.Merger;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.datakernel.stream.StreamProducers.ofIterable;

public final class StorageNodeTreeMap<K extends Comparable<K>, V> implements StorageNode<K, V> {
	private final Eventloop eventloop;
	private final Merger<KeyValue<K, V>> merger;

	private final TreeMap<K, V> values;

	public StorageNodeTreeMap(Eventloop eventloop, TreeMap<K, V> values, Merger<KeyValue<K, V>> merger) {
		this.eventloop = eventloop;
		this.merger = merger;
		this.values = values;
	}

	@Override
	public CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(java.util.function.Predicate<K> predicate) {
		assert eventloop.inEventloopThread();
		final Set<Map.Entry<K, V>> entries = Maps.filterKeys(values, predicate::test).entrySet();
		final List<KeyValue<K, V>> output = entries.stream()
				.map(input -> new KeyValue<>(input.getKey(), input.getValue()))
				.collect(Collectors.toList());

		return SettableStage.immediateStage(ofIterable(eventloop, output));
	}

	@Override
	public CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput() {
		return SettableStage.immediateStage(new AbstractStreamConsumer<KeyValue<K, V>>(eventloop) {
			@Override
			public StreamDataReceiver<KeyValue<K, V>> getDataReceiver() {
				return newValue -> {
					final K key = newValue.getKey();
					final KeyValue<K, V> oldValue = new KeyValue<>(key, values.get(key));
					values.put(key, merger.merge(newValue, oldValue).getValue());
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

	public V remove(K key) {
		assert eventloop.inEventloopThread();
		return values.remove(key);
	}

}