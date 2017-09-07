package io.datakernel.balancer;

import io.datakernel.storage.StorageNode;
import io.datakernel.stream.StreamConsumer;

import java.util.concurrent.CompletionStage;

public interface NodeBalancer<K, V> {

	CompletionStage<StreamConsumer<StorageNode.KeyValue<K, V>>> getPeers(StorageNode<K, V> node);
}
