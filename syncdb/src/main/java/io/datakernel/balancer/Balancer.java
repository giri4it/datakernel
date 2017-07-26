package io.datakernel.balancer;

import io.datakernel.async.ResultCallback;
import io.datakernel.storage.StorageNode;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.StreamConsumer;

import java.util.List;

public interface Balancer<K, V> {

	void getPeers(StorageNode<K, V> node, K key, ResultCallback<List<StreamConsumer<KeyValue<K, V>>>> callback);
}
