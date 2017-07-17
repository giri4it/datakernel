package io.datakernel.predicate;

import java.util.BitSet;
import java.util.List;

public class BloomFilter<T> {
	private final List<? extends HashFunction<T>> hashFunctions;
	private final BitSet bitSet;

	public BloomFilter(BitSet bitSet, List<? extends HashFunction<T>> hashFunctions) {
		this.hashFunctions = hashFunctions;
		this.bitSet = bitSet;
	}

	public BloomFilter(List<? extends HashFunction<T>> hashFunctions) {
		this(new BitSet(), hashFunctions);
	}

	public void add(T item) {
		for (HashFunction<T> hashFunction : hashFunctions) {
			bitSet.or(hashFunction.hashCode(item));
		}
	}

	public boolean contains(T item) {
		for (HashFunction<T> hashFunction : hashFunctions) {
			final BitSet hashCode = hashFunction.hashCode(item);
			for (int i = hashCode.nextSetBit(0); i >= 0; i = hashCode.nextSetBit(i+1)) {
				if (i == Integer.MAX_VALUE) break;
				if (!bitSet.get(i)) return false;
			}
		}
		return true;
	}

	public byte[] toByteArray() {
		return bitSet.toByteArray();
	}

	public long[] toLongArray() {
		return bitSet.toLongArray();
	}
}
