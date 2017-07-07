package storage;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.*;
import java.util.Map.Entry;

import static java.util.Arrays.asList;

public class DataStorageMerge implements HasSortedStream<Entry<Integer, Set<String>>>, HasInput<Entry<Integer, Set<String>>> {

	private static final Function<Entry<Integer, Set<String>>, Integer> TO_KEY = new Function<Entry<Integer, Set<String>>, Integer>() {
		@Override
		public Integer apply(Entry<Integer, Set<String>> entry) {
			return entry.getKey();
		}
	};

	private final Reducer<Integer, Entry<Integer, Set<String>>, Entry<Integer, Set<String>>, Entry<Integer, Set<String>>> reducer;
	private final Map<Integer, Set<String>> values = new TreeMap<>();
	private final Eventloop eventloop;

	public DataStorageMerge(Eventloop eventloop, Reducer<Integer, Entry<Integer, Set<String>>, Entry<Integer, Set<String>>, Entry<Integer, Set<String>>> reducer) {
		this.eventloop = eventloop;
		this.reducer = reducer;
	}

	private StreamProducer<Entry<Integer, Set<String>>> mergeStreams(final List<HasSortedStream<Entry<Integer, Set<String>>>> streams,
	                                                                 final Predicate<Entry<Integer, Set<String>>> predicate) {
		final StreamReducer<Integer, Entry<Integer, Set<String>>, Entry<Integer, Set<String>>> streamReducer =
				StreamReducer.create(eventloop, Ordering.<Integer>natural());

		for (HasSortedStream<Entry<Integer, Set<String>>> stream : streams) {
			stream.getSortedStream(predicate).streamTo(streamReducer.newInput(TO_KEY, reducer));
		}

		return streamReducer.getOutput();
	}

	@SafeVarargs
	public final void merge(final Predicate<Entry<Integer, Set<String>>> predicate, final HasSortedStream<Entry<Integer, Set<String>>>... streams) {
		assert eventloop.inEventloopThread();
		mergeStreams(asList(streams), predicate).streamTo(new AbstractStreamConsumer<Entry<Integer, Set<String>>>(eventloop) {
			@Override
			public StreamDataReceiver<Entry<Integer, Set<String>>> getDataReceiver() {
				return new StreamDataReceiver<Entry<Integer, Set<String>>>() {
					@Override
					public void onData(Entry<Integer, Set<String>> item) {
						values.put(item.getKey(), item.getValue());
					}
				};
			}
		});
	}

	@Override
	public StreamProducer<Entry<Integer, Set<String>>> getSortedStream(final Predicate<Entry<Integer, Set<String>>> filter) {
		assert eventloop.inEventloopThread();
		return StreamProducers.ofIterable(eventloop, Sets.filter(values.entrySet(), filter));
	}

	@Override
	public StreamConsumer<Entry<Integer, Set<String>>> getInput() {
		return new AbstractStreamConsumer<Entry<Integer, Set<String>>>(eventloop) {
			@Override
			public StreamDataReceiver<Entry<Integer, Set<String>>> getDataReceiver() {
				return new StreamDataReceiver<Entry<Integer, Set<String>>>() {
					@Override
					public void onData(Entry<Integer, Set<String>> item) {
						values.put(item.getKey(), item.getValue());
					}
				};
			}
		};
	}
}
