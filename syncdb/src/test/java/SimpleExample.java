import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;
import org.slf4j.LoggerFactory;
import storage.DataStorageMerge;
import storage.DataStorageSimple;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;

public class SimpleExample {

	public static final Predicate<Map.Entry<Integer, Set<String>>> ALWAYS_TRUE = Predicates.<Map.Entry<Integer,Set<String>>>alwaysTrue();

	static {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
	}

	private static DataStorageSimple createSimpleStorage(Eventloop eventloop, final int id, final String... values) {
		return new DataStorageSimple(eventloop, new TreeMap<Integer, Set<String>>() {{
			put(id, newHashSet(values));
		}});
	}

	private static Map.Entry<Integer, Set<String>> createEntry(final int id, final String... values) {
		return new AbstractMap.SimpleEntry<Integer, Set<String>>(id, Sets.newHashSet(values));
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
		dataStorageMerge1.merge(ALWAYS_TRUE, dataStorage1, dataStorage3);
		dataStorageMerge2.merge(ALWAYS_TRUE, dataStorage2, dataStorage3);

		eventloop.run();

		System.out.println("merger1 " + toString(eventloop, dataStorageMerge1.getSortedStream(ALWAYS_TRUE)));
		System.out.println("merger2 " + toString(eventloop, dataStorageMerge2.getSortedStream(ALWAYS_TRUE)));

		dataStorageMerge1.merge(ALWAYS_TRUE, dataStorageMerge1, dataStorageMerge2);

		System.out.println("--------------------------------------------");
		System.out.println("merger1 " + toString(eventloop, dataStorageMerge1.getSortedStream(ALWAYS_TRUE)));
		System.out.println("merger2 " + toString(eventloop, dataStorageMerge2.getSortedStream(ALWAYS_TRUE)));

		System.out.println("--------------------------------------------");
		System.out.println("storage1 " + toString(eventloop, dataStorage1.getSortedStream(ALWAYS_TRUE)));

		dataStorageMerge1.getSortedStream(ALWAYS_TRUE).streamTo(dataStorage1.getInput());

		eventloop.run();

		System.out.println("storage1 " + toString(eventloop, dataStorage1.getSortedStream(ALWAYS_TRUE)));
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
