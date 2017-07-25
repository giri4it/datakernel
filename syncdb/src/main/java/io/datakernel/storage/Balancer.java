package io.datakernel.storage;

import io.datakernel.async.ResultCallback;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.util.List;

public interface Balancer<K, V> {

	void getPeers(StreamProducer<KeyValue<K, V>> producer, K key, ResultCallback<List<StreamConsumer<KeyValue<K, V>>>> callback);
}
