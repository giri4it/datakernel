package io.datakernel.predicate;

import com.google.common.base.Predicate;

public class BloomFilterPredicate<T> implements Predicate<T> {
	private final BloomFilter<T> bloomFilter;

	public BloomFilterPredicate(BloomFilter<T> bloomFilter) {
		this.bloomFilter = bloomFilter;
	}

	@Override
	public boolean apply(T input) {
		return bloomFilter.contains(input);
	}

	public BloomFilter<T> getBloomFilter() {
		return bloomFilter;
	}
}
