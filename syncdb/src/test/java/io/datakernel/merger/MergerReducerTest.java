package io.datakernel.merger;

import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.processor.StreamReducers;
import org.junit.Test;
import io.datakernel.storage.HasSortedStreamProducer.KeyValue;

import java.util.Set;
import java.util.TreeSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MergerReducerTest {

	private static KeyValue<Integer, Set<String>> create(int key, String... values) {
		return new KeyValue<Integer, Set<String>>(key, new TreeSet<>(asList(values)));
	}

	@Test
	public void testMergeSortReducer() {
		final MergerReducer<Integer, Set<String>, Void> mergerReducer = new MergerReducer<>(new ReduceMerger());

		assertEquals(create(1, "a", "b"), mergerReducer.merge(create(1, "a", "b"), create(1, "b", "c")));
		assertEquals(create(1, "a"), mergerReducer.merge(create(1, "a"), create(1)));
		assertEquals(create(1), mergerReducer.merge(create(1), null));

	}

	@Test
	public void testAccumulatorReducer() {
		final MergerReducer<Integer, Set<String>, KeyValue<Integer, Set<String>>> unionMerger = new MergerReducer<>(new ReducerUnion().inputToOutput());

		assertEquals(create(1, "a", "b", "c"), unionMerger.merge(create(1, "a", "b"), create(1, "b", "c")));
		assertEquals(create(1, "a"), unionMerger.merge(create(1, "a"), create(1)));
		assertEquals(create(1), unionMerger.merge(create(1), null));
	}

	private static class ReducerUnion extends StreamReducers.ReducerToAccumulator<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> {

		@Override
		public KeyValue<Integer, Set<String>> createAccumulator(Integer key) {
			return new KeyValue<Integer, Set<String>>(key, new TreeSet<String>());
		}

		@Override
		public KeyValue<Integer, Set<String>> accumulate(KeyValue<Integer, Set<String>> accumulator, KeyValue<Integer, Set<String>> value) {
			if (value != null) accumulator.getValue().addAll(value.getValue());
			return accumulator;
		}
	}

	private static class ReduceMerger implements StreamReducers.Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, Void> {

		@Override
		public Void onFirstItem(StreamDataReceiver<KeyValue<Integer, Set<String>>> stream, Integer key, KeyValue<Integer, Set<String>> firstValue) {
			stream.onData(firstValue);
			return null;
		}

		@Override
		public Void onNextItem(StreamDataReceiver<KeyValue<Integer, Set<String>>> stream, Integer key, KeyValue<Integer, Set<String>> nextValue, Void accumulator) {
			stream.onData(nextValue);
			return null;
		}

		@Override
		public void onComplete(StreamDataReceiver<KeyValue<Integer, Set<String>>> stream, Integer key, Void accumulator) {

		}
	}

}