package io.datakernel.balancer;

import io.datakernel.async.ResultCallback;
import io.datakernel.storage.StorageNode;

import java.util.List;

public interface NodeSelector<K, V> {

	void selectNodes(StorageNode<K, V> initNode, ResultCallback<List<StorageNode<K, V>>> callback);
}
