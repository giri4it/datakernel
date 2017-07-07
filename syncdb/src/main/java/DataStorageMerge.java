import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamConsumer;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Arrays.asList;

public class DataStorageMerge implements DataStorage<Entry<Integer, Set<String>>> {

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

	private StreamProducer<Entry<Integer, Set<String>>> mergeStreams(final List<StreamProducer<Entry<Integer, Set<String>>>> streams) {
		final StreamReducer<Integer, Entry<Integer, Set<String>>, Entry<Integer, Set<String>>> streamReducer =
				StreamReducer.create(eventloop, Ordering.<Integer>natural());

		for (StreamProducer<Entry<Integer, Set<String>>> stream : streams) {
			stream.streamTo(streamReducer.newInput(TO_KEY, reducer));
		}

		return streamReducer.getOutput();
	}

	@SafeVarargs
	public final void merge(StreamProducer<Entry<Integer, Set<String>>>... streams) {
		assert eventloop.inEventloopThread();
		mergeStreams(asList(streams)).streamTo(new AbstractStreamConsumer<Entry<Integer, Set<String>>>(eventloop) {
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
	public StreamProducer<Entry<Integer, Set<String>>> getSortedStream() {
		assert eventloop.inEventloopThread();
		return StreamProducers.ofIterable(eventloop, values.entrySet());
	}
}
