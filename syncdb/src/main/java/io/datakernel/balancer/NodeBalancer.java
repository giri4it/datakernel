package io.datakernel.balancer;

import io.datakernel.async.ResultCallback;
import io.datakernel.storage.StorageNode;
import io.datakernel.stream.StreamConsumer;

public interface NodeBalancer<K, V> {

	void getPeers(StorageNode<K, V> node, ResultCallback<StreamConsumer<StorageNode.KeyValue<K, V>>> callback);
}
