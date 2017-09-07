package io.datakernel.predicate;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.BitSet;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

public class BloomFilterTest {
	private static final HashFunction<String> SIMPLE_HASH = new HashFunction<String>() {
		@Override
		public BitSet hashCode(String item) {
			return BitSet.valueOf(Ints.toByteArray(item.hashCode()));
		}
	};

	private static final HashFunction<String> MURMUR_HASH = new HashFunction<String>() {
		@Override
		public BitSet hashCode(String item) {
			return BitSet.valueOf(Hashing.murmur3_32().newHasher().putString(item, Charsets.UTF_8).hash().asBytes());
		}
	};

	@Test
	public void testContains() {
		final BloomFilter<String> bloomFilter = new BloomFilter<>(singletonList(SIMPLE_HASH));
		bloomFilter.add("abc");

		assertTrue(bloomFilter.contains("abc"));
		assertFalse(bloomFilter.contains("abcd"));
	}

	@Test
	public void testCollision() {
		final BloomFilter<String> bloomFilter = new BloomFilter<>(singletonList(SIMPLE_HASH));
		bloomFilter.add("FB");

		assertEquals("FB".hashCode(), "Ea".hashCode());
		assertTrue(bloomFilter.contains("FB"));
		assertTrue(bloomFilter.contains("Ea"));
	}

	@Test
	public void testResolveCollision() {
		final BloomFilter<String> bloomFilter = new BloomFilter<>(asList(SIMPLE_HASH, MURMUR_HASH));
		bloomFilter.add("FB");

		assertEquals("FB".hashCode(), "Ea".hashCode());
		assertTrue(bloomFilter.contains("FB"));
		assertFalse(bloomFilter.contains("Ea"));
	}

	@Test
	public void testFromBitSet() {
		final BitSet bitSet = BitSet.valueOf(Ints.toByteArray("1234".hashCode()));
		final BloomFilter<String> bloomFilter = new BloomFilter<>(bitSet, singletonList(SIMPLE_HASH));

		assertTrue(bloomFilter.contains("1234"));
		assertArrayEquals(bloomFilter.toByteArray(), bitSet.toByteArray());
		assertArrayEquals(bloomFilter.toLongArray(), bitSet.toLongArray());
	}

}