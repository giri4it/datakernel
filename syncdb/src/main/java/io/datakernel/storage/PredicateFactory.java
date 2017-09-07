package io.datakernel.storage;

import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

public interface PredicateFactory<K, V> {

	CompletionStage<Predicate<K>> create(StorageNode<K, V> node);
}
