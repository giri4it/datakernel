import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.async.AsyncRunnable;
import io.datakernel.async.AsyncRunnables;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.IgnoreCompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import org.slf4j.LoggerFactory;
import storage.DataStorageMerger;
import storage.DataStorageSimple;
import storage.HasSortedStream;
import storage.HasSortedStream.KeyValue;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;

public class SimpleExample {

	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
	private static final Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> UNION_REDUCER =
			TestUnion.getInstance().inputToOutput();

	private static AsyncRunnable synchronize(final DataStorageSimple dataStorage) {
		return new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				dataStorage.synchronize(callback);
			}
		};
	}

	private static HasSortedStream<Integer, Set<String>> sorterStream(final StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		return new HasSortedStream<Integer, Set<String>>() {
			@Override
			public StreamProducer<KeyValue<Integer, Set<String>>> getSortedStream(Predicate<Integer> predicate) {
				return producer;
			}
		};
	}

	private static DataStorageSimple createSimpleStorage(Eventloop eventloop, final KeyValue<Integer, Set<String>> value,
	                                                     List<? extends HasSortedStream<Integer, Set<String>>> peers,
	                                                     Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer,
	                                                     Predicate<Integer> keyFilter) {
		return new DataStorageSimple(eventloop, new TreeMap<Integer, Set<String>>() {{
			put(value.getKey(), newHashSet(value.getValue()));
		}}, reducer, peers, keyFilter);
	}

	private static KeyValue<Integer, Set<String>>newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	private static void printStreams(Eventloop eventloop, DataStorageSimple dataStorage1,
	                                 DataStorageSimple dataStorage2, DataStorageSimple dataStorage3,
	                                 DataStorageMerger dataStorageMerge1, DataStorageMerger dataStorageMerge2) {
		System.out.println("--------------------------------------------");
		System.out.println("storage1\t" + toString(eventloop, dataStorage1.getSortedStream(ALWAYS_TRUE)));
		System.out.println("storage2\t" + toString(eventloop, dataStorage2.getSortedStream(ALWAYS_TRUE)));
		System.out.println("storage3\t" + toString(eventloop, dataStorage3.getSortedStream(ALWAYS_TRUE)));
		System.out.println();
		System.out.println("merger1\t\t" + toString(eventloop, dataStorageMerge1.getSortedStream(ALWAYS_TRUE)));
		System.out.println("merger2\t\t" + toString(eventloop, dataStorageMerge2.getSortedStream(ALWAYS_TRUE)));
		System.out.println();
	}

	private static String toString(Eventloop eventloop, StreamProducer<KeyValue<Integer, Set<String>>> producer) {
		final StreamConsumers.ToList<KeyValue<Integer, Set<String>>> toList = StreamConsumers.toList(eventloop);
		producer.streamTo(toList);
		eventloop.run();
		return toList.getList().toString();
	}

	static {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
	}

	@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
	public static void main(String[] args) {
		final Eventloop eventloop = Eventloop.create();

		final KeyValue<Integer, Set<String>> value1 = newKeyValue(1, "ivan:cars", "ivan:table");
		final KeyValue<Integer, Set<String>> value2 = newKeyValue(1, "ivan:phones", "ivan:mouse");
		final KeyValue<Integer, Set<String>> value3 = newKeyValue(5, "jim:music", "jim:cup");

		final List<HasSortedStream<Integer, Set<String>>> sortedStreams1 = asList(sorterStream(ofValue(eventloop, value1)));
		final List<HasSortedStream<Integer, Set<String>>> sortedStreams2 = asList(sorterStream(ofValue(eventloop, value2)));
		final List<HasSortedStream<Integer, Set<String>>> sortedStreams3 = asList(sorterStream(ofValue(eventloop, value3)));

		final KeyValue<Integer, Set<String>> data1 = newKeyValue(1, "ivan:cars", "ivan:dolls");
		final KeyValue<Integer, Set<String>> data2 = newKeyValue(1, "ivan:cars", "ivan:phones");
		final KeyValue<Integer, Set<String>> data3 = newKeyValue(5, "jim:books", "jim:music");

		final DataStorageSimple dataStorage1 = createSimpleStorage(eventloop, data1, sortedStreams1, UNION_REDUCER, ALWAYS_TRUE);
		final DataStorageSimple dataStorage2 = createSimpleStorage(eventloop, data2, sortedStreams2, UNION_REDUCER, ALWAYS_TRUE);
		final DataStorageSimple dataStorage3 = createSimpleStorage(eventloop, data3, sortedStreams3, UNION_REDUCER, ALWAYS_TRUE);

		final DataStorageMerger dataStorageMerge1 = new DataStorageMerger(eventloop, UNION_REDUCER, asList(dataStorage1, dataStorage2));
		final DataStorageMerger dataStorageMerge2 = new DataStorageMerger(eventloop, UNION_REDUCER, asList(dataStorage2, dataStorage3));

		eventloop.run();

		printStreams(eventloop, dataStorage1, dataStorage2, dataStorage3, dataStorageMerge1, dataStorageMerge2);

		AsyncRunnables.runInParallel(eventloop, asList(
				synchronize(dataStorage1),
				synchronize(dataStorage2),
				synchronize(dataStorage3)))
				.run(IgnoreCompletionCallback.create());

		eventloop.run();

		printStreams(eventloop, dataStorage1, dataStorage2, dataStorage3, dataStorageMerge1, dataStorageMerge2);

	}

	private static class TestUnion extends StreamReducers.ReducerToAccumulator<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> {
		private static final TestUnion INSTANCE = new TestUnion();

		private TestUnion() {

		}

		static TestUnion getInstance() {
			return INSTANCE;
		}

		@Override
		public KeyValue<Integer, Set<String>> createAccumulator(Integer key) {
			return new KeyValue<Integer, Set<String>>(key, new TreeSet<String>());
		}

		@Override
		public KeyValue<Integer, Set<String>> accumulate(KeyValue<Integer, Set<String>> accumulator, KeyValue<Integer, Set<String>> value) {
			accumulator.getValue().addAll(value.getValue());
			return accumulator;
		}
	}
}
