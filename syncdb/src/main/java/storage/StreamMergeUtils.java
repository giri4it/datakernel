package storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;
import storage.HasSortedStream.KeyValue;

import java.util.Comparator;

public class StreamMergeUtils {
	private StreamMergeUtils() {

	}

	public static <K, V, A> StreamProducer<KeyValue<K, V>> mergeStreams(
			final Eventloop eventloop,
			final Comparator<K> keyComparator,
			final Function<KeyValue<K, V>, K> keyFunction,
			final StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer,
			final Iterable<? extends HasSortedStream<K, V>> streams,
			final Predicate<K> predicate) {

		final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, keyComparator);

		for (HasSortedStream<K, V> stream : streams) {
			stream.getSortedStream(predicate).streamTo(streamReducer.newInput(keyFunction, reducer));
		}

		return streamReducer.getOutput();
	}
}
