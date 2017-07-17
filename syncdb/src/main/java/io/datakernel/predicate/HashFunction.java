package io.datakernel.predicate;

import java.util.BitSet;

public interface HashFunction<T> {
	BitSet hashCode(T item);
}
