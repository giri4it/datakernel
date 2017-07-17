package io.datakernel.predicate;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public class BloomFilterPredicateTypeAdapter<T> extends TypeAdapter<BloomFilterPredicate<T>> {

	public static final String NAME = "bloomFilter";
	public static final String BLOOM_SIZE = "size";
	public static final String BLOOM_ARRAY = "array";

	private final List<? extends HashFunction<T>> hashFunctions;

	public BloomFilterPredicateTypeAdapter(List<? extends HashFunction<T>> hashFunctions) {
		this.hashFunctions = hashFunctions;
	}

	@Override
	public void write(JsonWriter jsonWriter, BloomFilterPredicate<T> bloomFilterPredicate) throws IOException {
		jsonWriter.beginObject();
		jsonWriter.name(BLOOM_SIZE).value(bloomFilterPredicate.getBloomFilter().toLongArray().length);
		jsonWriter.name(BLOOM_ARRAY);
		jsonWriter.beginArray();
		for (long value : bloomFilterPredicate.getBloomFilter().toLongArray()) jsonWriter.value(value);
		jsonWriter.endArray();
		jsonWriter.endObject();
	}

	@Override
	public BloomFilterPredicate<T> read(JsonReader jsonReader) throws IOException {
		jsonReader.beginObject();
		jsonReader.nextName();

		final int bloomSize = jsonReader.nextInt();
		final long[] longs = new long[bloomSize];

		jsonReader.nextName();
		jsonReader.beginArray();
		for (int i = 0; i < longs.length; i++) longs[i] = jsonReader.nextLong();
		jsonReader.endArray();
		jsonReader.endObject();

		return new BloomFilterPredicate<>(new BloomFilter<>(BitSet.valueOf(longs), hashFunctions));
	}
}
