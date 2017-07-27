package io.datakernel.storage;

import com.google.common.base.Predicate;
import io.datakernel.async.ResultCallback;

public interface PredicateFactory<K, V> {

	void create(StorageNode<K, V> node, ResultCallback<Predicate<K>> callback);
}
