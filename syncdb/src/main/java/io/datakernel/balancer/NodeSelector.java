package io.datakernel.balancer;

import io.datakernel.storage.StorageNode;

import java.util.List;
import java.util.concurrent.CompletionStage;

public interface NodeSelector<K, V> {

	CompletionStage<List<StorageNode<K, V>>> selectNodes(StorageNode<K, V> initNode);
}
