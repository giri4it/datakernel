package io.datakernel.predicate;

import java.util.function.Predicate;

public class BloomFilterPredicate<T> implements Predicate<T> {
	private final BloomFilter<T> bloomFilter;

	public BloomFilterPredicate(BloomFilter<T> bloomFilter) {
		this.bloomFilter = bloomFilter;
	}

	@Override
	public boolean test(T t) {
		return bloomFilter.contains(t);
	}

	public BloomFilter<T> getBloomFilter() {
		return bloomFilter;
	}
}
