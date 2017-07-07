package storage;

import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers.Reducer;

import java.util.List;
import java.util.Set;

import static storage.KeyValueUtils.toKey;
import static storage.StreamMergeUtils.mergeStreams;

public class DataStorageMerger implements HasSortedStream<Integer, Set<String>> {

	private static final Ordering<Integer> ORDERING = Ordering.natural();

	private final Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer;
	private final List<? extends HasSortedStream<Integer, Set<String>>> peers;
	private final Eventloop eventloop;

	public DataStorageMerger(Eventloop eventloop,
	                         Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer,
	                         List<? extends HasSortedStream<Integer, Set<String>>> peers) {

		this.eventloop = eventloop;
		this.reducer = reducer;
		this.peers = peers;
	}

	@Override
	public StreamProducer<KeyValue<Integer, Set<String>>> getSortedStream(final Predicate<Integer> filter) {
		assert eventloop.inEventloopThread();
		return mergeStreams(eventloop, ORDERING, toKey(), reducer, peers, filter);
	}

}
