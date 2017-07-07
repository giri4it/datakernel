import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DataStorageSimple implements DataStorage<Map.Entry<Integer, Set<String>>> {
	private final TreeMap<Integer, Set<String>> values;
	private final Eventloop eventloop;

	public DataStorageSimple(Eventloop eventloop, TreeMap<Integer, Set<String>> values) {
		this.eventloop = eventloop;
		this.values = values;
	}

	@Override
	public StreamProducer<Map.Entry<Integer, Set<String>>> getSortedStream() {
		assert eventloop.inEventloopThread();

		return StreamProducers.ofIterable(eventloop, values.entrySet());
	}
}
