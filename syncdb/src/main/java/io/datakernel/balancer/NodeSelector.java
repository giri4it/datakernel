package io.datakernel.balancer;

import io.datakernel.async.ResultCallback;
import io.datakernel.storage.StorageNode;

public interface NodeSelector<K, V> {

	void selectNodes(StorageNode<K, V> initNode, ResultCallback<Iterable<StorageNode<K, V>>> callback);
}
