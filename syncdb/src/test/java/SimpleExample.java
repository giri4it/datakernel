import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;

public class SimpleExample {

	private static DataStorageSimple createSimpleStorage(Eventloop eventloop, final int id, final String... values) {
		return new DataStorageSimple(eventloop, new TreeMap<Integer, Set<String>>() {{
			put(id, newHashSet(values));
		}});
	}

	private static String toString(Eventloop eventloop, StreamProducer<Map.Entry<Integer, Set<String>>> producer) {
		final StreamConsumers.ToList<Map.Entry<Integer, Set<String>>> toList = StreamConsumers.toList(eventloop);
		producer.streamTo(toList);
		eventloop.run();
		return toList.getList().toString();
	}

	public static void main(String[] args) {
		final Eventloop eventloop = Eventloop.create();

		final DataStorageSimple dataStorage1 = createSimpleStorage(eventloop, 1, "ivan:cars", "ivan:dolls");
		final DataStorageSimple dataStorage2 = createSimpleStorage(eventloop, 1, "ivan:cars", "ivan:phones");
		final DataStorageSimple dataStorage3 = createSimpleStorage(eventloop, 5, "jim:books", "jim:music");

		final DataStorageMerge dataStorageMerge1 = new DataStorageMerge(eventloop, new TestUnion().inputToOutput());
		final DataStorageMerge dataStorageMerge2 = new DataStorageMerge(eventloop, new TestUnion().inputToOutput());
//
		dataStorageMerge1.merge(dataStorage1.getSortedStream(), dataStorage3.getSortedStream()); // 1, 3
		dataStorageMerge2.merge(dataStorage2.getSortedStream(), dataStorage3.getSortedStream()); // 2, 3

		eventloop.run();

		System.out.println(toString(eventloop, dataStorageMerge1.getSortedStream()));
		System.out.println(toString(eventloop, dataStorageMerge2.getSortedStream()));

		dataStorageMerge1.merge(dataStorageMerge1.getSortedStream(), dataStorageMerge2.getSortedStream());

		System.out.println("--------------------------------------------");
		System.out.println(toString(eventloop, dataStorageMerge1.getSortedStream()));
		System.out.println(toString(eventloop, dataStorageMerge2.getSortedStream()));
	}

	private static class TestUnion extends StreamReducers.ReducerToAccumulator<Integer, Map.Entry<Integer, Set<String>>, Map.Entry<Integer, Set<String>>> {

		@Override
		public Map.Entry<Integer, Set<String>> createAccumulator(Integer key) {
			return new AbstractMap.SimpleEntry<Integer, Set<String>>(key, new HashSet<String>());
		}

		@Override
		public Map.Entry<Integer, Set<String>> accumulate(Map.Entry<Integer, Set<String>> accumulator, Map.Entry<Integer, Set<String>> value) {
			accumulator.getValue().addAll(value.getValue());
			return accumulator;
		}
	}
}
