package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.sun.istack.internal.Nullable;

public interface PredicateFactory<K, V> {
	@Nullable Predicate<K> create(StorageNode<K, V> node);
}
