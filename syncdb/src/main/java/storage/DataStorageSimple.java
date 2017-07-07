package storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.*;

import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singletonList;
import static storage.KeyValueUtils.toKey;
import static storage.StreamMergeUtils.mergeStreams;

// interface DataStorage extends HasSortedStream<Integer, Set<String>>, Synchronizer ???
public class DataStorageSimple implements HasSortedStream<Integer, Set<String>>, Synchronizer {

	private static final Ordering<Integer> ORDERING = Ordering.natural();

	private static final Function<Map.Entry<Integer, Set<String>>, KeyValue<Integer, Set<String>>> TO_KEY_VALUE = new Function<Map.Entry<Integer, Set<String>>, KeyValue<Integer, Set<String>>>() {
		@Override
		public KeyValue<Integer, Set<String>> apply(Map.Entry<Integer, Set<String>> input) {
			return new KeyValue<>(input.getKey(), input.getValue());
		}
	};


	private final Eventloop eventloop;
	private final Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer;
	private final List<? extends HasSortedStream<Integer, Set<String>>> peers;
	private final Predicate<Integer> keyFilter;

	private final TreeMap<Integer, Set<String>> values;

	public DataStorageSimple(Eventloop eventloop, TreeMap<Integer, Set<String>> values,
	                         Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer,
	                         List<? extends HasSortedStream<Integer, Set<String>>> peers,
	                         Predicate<Integer> keyFilter) {
		this.eventloop = eventloop;
		this.values = values;
		this.reducer = reducer;
		this.peers = peers;
		this.keyFilter = keyFilter;
	}

	@Override
	public StreamProducer<KeyValue<Integer, Set<String>>> getSortedStream(Predicate<Integer> predicate) {
		assert eventloop.inEventloopThread();
		return StreamProducers.ofIterable(eventloop, Iterables.transform(Maps.filterKeys(values, predicate).entrySet(), TO_KEY_VALUE));
	}

	@Override
	public void synchronize(final CompletionCallback callback) {
		final Iterable<HasSortedStream<Integer, Set<String>>> peers = concat(this.peers, singletonList(this));
		final StreamProducer<KeyValue<Integer, Set<String>>> producer = mergeStreams(eventloop, ORDERING, toKey(), reducer, peers, keyFilter);
		producer.streamTo(new AbstractStreamConsumer<KeyValue<Integer, Set<String>>>(eventloop) {
			@Override
			public StreamDataReceiver<KeyValue<Integer, Set<String>>> getDataReceiver() {
				return new StreamDataReceiver<KeyValue<Integer, Set<String>>>() {
					@Override
					public void onData(KeyValue<Integer, Set<String>> item) {
						values.put(item.getKey(), item.getValue());
					}
				};
			}

			@Override
			protected void onEndOfStream() {
				callback.setComplete();
			}
		});
	}
}
