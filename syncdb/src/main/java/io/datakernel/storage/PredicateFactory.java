package io.datakernel.storage;

import com.google.common.base.Predicate;

public interface PredicateFactory<K, V> {
	Predicate<K> create(StorageNode<K, V> node);
}
