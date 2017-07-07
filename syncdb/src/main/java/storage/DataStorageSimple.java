package storage;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DataStorageSimple implements HasSortedStream<Map.Entry<Integer, Set<String>>>, HasInput<Map.Entry<Integer, Set<String>>> {
	private final TreeMap<Integer, Set<String>> values;
	private final Eventloop eventloop;

	public DataStorageSimple(Eventloop eventloop, TreeMap<Integer, Set<String>> values) {
		this.eventloop = eventloop;
		this.values = values;
	}

	@Override
	public StreamProducer<Map.Entry<Integer, Set<String>>> getSortedStream(Predicate<Map.Entry<Integer, Set<String>>> filter) {
		assert eventloop.inEventloopThread();
		return StreamProducers.ofIterable(eventloop, Sets.filter(values.entrySet(), filter));
	}

	@Override
	public StreamConsumer<Map.Entry<Integer, Set<String>>> getInput() {
		return new AbstractStreamConsumer<Map.Entry<Integer, Set<String>>>(eventloop) {
			@Override
			public StreamDataReceiver<Map.Entry<Integer, Set<String>>> getDataReceiver() {
				return new StreamDataReceiver<Map.Entry<Integer, Set<String>>>() {
					@Override
					public void onData(Map.Entry<Integer, Set<String>> item) {
						values.put(item.getKey(), item.getValue());
					}
				};
			}
		};
	}
}
