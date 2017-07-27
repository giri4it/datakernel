package io.datakernel.balancer;

import io.datakernel.storage.StorageNode;

public interface NodeSelector<K, V> {

	Iterable<StorageNode<K, V>> selectNodes(StorageNode<K, V> initNode);
}
