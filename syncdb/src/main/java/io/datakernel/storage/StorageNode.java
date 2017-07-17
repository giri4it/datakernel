package io.datakernel.storage;

public interface StorageNode<K, V> extends HasSortedStreamProducer<K, V>, HasSortedStreamConsumer<K, V> {

}
