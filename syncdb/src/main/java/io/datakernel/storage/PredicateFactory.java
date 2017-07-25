package io.datakernel.storage;

import com.google.common.base.Predicate;

public interface PredicateFactory<T> {
	<K, V> Predicate<T> create(StorageNode<K, V> node);
}
