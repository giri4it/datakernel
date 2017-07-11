package io.datakernel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Sets;
import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import org.slf4j.LoggerFactory;
import io.datakernel.storage.DataStorageMerger;
import io.datakernel.storage.DataStorageTreeMap;
import io.datakernel.storage.HasSortedStream;
import io.datakernel.storage.HasSortedStream.KeyValue;

import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static io.datakernel.stream.StreamProducers.ofValue;
import static java.util.Arrays.asList;

public class SimpleExample {

	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();
	private static final Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> UNION_REDUCER =
			TestUnion.getInstance().inputToOutput();

	private static AsyncRunnable synchronize(final DataStorageTreeMap dataStorage) {
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
			public void getSortedStream(Predicate<Integer> predicate, ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				callback.setResult(producer);
			}
		};
	}

	private static DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> createSimpleStorage(
			final Eventloop eventloop,
			final KeyValue<Integer, Set<String>> value,
			final List<? extends HasSortedStream<Integer, Set<String>>> peers,
			final Reducer<Integer, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>, KeyValue<Integer, Set<String>>> reducer,
			final Predicate<Integer> keyFilter) {

		return new DataStorageTreeMap<>(eventloop, new TreeMap<Integer, Set<String>>(){{
			put(value.getKey(), value.getValue());
		}}, peers, reducer, keyFilter);
	}

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	private static void printStreams(final Eventloop eventloop, final DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorage1,
	                                 final DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorage2,
	                                 final DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorage3,
	                                 final DataStorageMerger<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorageMerge1,
	                                 final DataStorageMerger<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorageMerge2) {
		System.out.println("--------------------------------------------");
		AsyncCallables.callAll(eventloop, asList(
				getSortedStream(dataStorage1),
				getSortedStream(dataStorage2),
				getSortedStream(dataStorage3),
				getSortedStream(dataStorageMerge1),
				getSortedStream(dataStorageMerge2)))
		.call(new AssertingResultCallback<List<StreamProducer<KeyValue<Integer, Set<String>>>>>() {
			@Override
			protected void onResult(List<StreamProducer<KeyValue<Integer, Set<String>>>> result) {
				System.out.println("storage1\t" + SimpleExample.toString(eventloop, result.get(0)));
				System.out.println("storage2\t" + SimpleExample.toString(eventloop, result.get(1)));
				System.out.println("storage3\t" + SimpleExample.toString(eventloop, result.get(2)));
				System.out.println();
				System.out.println("merger1\t\t" + SimpleExample.toString(eventloop, result.get(3)));
				System.out.println("merger2\t\t" + SimpleExample.toString(eventloop, result.get(4)));
				System.out.println();
			}

		});
	}

	private static AsyncCallable<StreamProducer<KeyValue<Integer, Set<String>>>> getSortedStream(final HasSortedStream<Integer, Set<String>> hasSortedStream) {
		return new AsyncCallable<StreamProducer<KeyValue<Integer, Set<String>>>>() {
			@Override
			public void call(ResultCallback<StreamProducer<KeyValue<Integer, Set<String>>>> callback) {
				hasSortedStream.getSortedStream(ALWAYS_TRUE, callback);
			}
		};
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

		final DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorage1 = createSimpleStorage(eventloop, data1, sortedStreams1, UNION_REDUCER, ALWAYS_TRUE);
		final DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorage2 = createSimpleStorage(eventloop, data2, sortedStreams2, UNION_REDUCER, ALWAYS_TRUE);
		final DataStorageTreeMap<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorage3 = createSimpleStorage(eventloop, data3, sortedStreams3, UNION_REDUCER, ALWAYS_TRUE);

		final DataStorageMerger<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorageMerge1 = new DataStorageMerger<>(eventloop, UNION_REDUCER, asList(dataStorage1, dataStorage2));
		final DataStorageMerger<Integer, Set<String>, KeyValue<Integer, Set<String>>> dataStorageMerge2 = new DataStorageMerger<>(eventloop, UNION_REDUCER, asList(dataStorage2, dataStorage3));

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
