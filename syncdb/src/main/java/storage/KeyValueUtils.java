package storage;

import com.google.common.base.Function;

import java.util.Set;

public class KeyValueUtils {
	private static final Function<HasSortedStream.KeyValue<Integer, Set<String>>, Integer> TO_KEY = new Function<HasSortedStream.KeyValue<Integer, Set<String>>, Integer>() {
		@Override
		public Integer apply(HasSortedStream.KeyValue<Integer, Set<String>> input) {
			return input.getKey();
		}
	};

	private KeyValueUtils() {

	}

	public static Function<HasSortedStream.KeyValue<Integer, Set<String>>, Integer> toKey() {
		return TO_KEY;
	}
}
