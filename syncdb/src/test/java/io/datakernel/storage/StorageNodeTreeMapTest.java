package io.datakernel.storage;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallbackFuture;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallbackFuture;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.merger.Merger;
import io.datakernel.storage.StorageNode.KeyValue;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.*;

public class StorageNodeTreeMapTest {
	private static final Predicate<Integer> ALWAYS_TRUE = Predicates.alwaysTrue();

	private Eventloop eventloop;
	private Merger<KeyValue<Integer, Set<String>>> merger;
	private TreeMap<Integer, Set<String>> state;
	private StorageNodeTreeMap<Integer, Set<String>> dataStorageTreeMap;

	private static KeyValue<Integer, Set<String>> newKeyValue(int key, String... value) {
		return new KeyValue<Integer, Set<String>>(key, Sets.newTreeSet(asList(value)));
	}

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
		state = new TreeMap<>();
		merger = new Merger<KeyValue<Integer, Set<String>>>() {
			@Override
			public KeyValue<Integer, Set<String>> merge(KeyValue<Integer, Set<String>> arg1, @Nullable KeyValue<Integer, Set<String>> arg2) {
				final TreeSet<String> set = new TreeSet<>(arg1.getValue());
				if (arg2.getValue() != null) set.addAll(arg2.getValue());
				return new KeyValue<Integer, Set<String>>(arg1.getKey(), set);
			}
		};
		setUpTreeStorage();
	}

	private void setUpTreeStorage() {
		dataStorageTreeMap = new StorageNodeTreeMap<>(eventloop, state, merger);
	}

	private <T> List<T> toList(StreamProducer<T> producer) {
		final StreamConsumers.ToList<T> streamToList = StreamConsumers.toList(eventloop);
		producer.streamTo(streamToList);
		eventloop.run();
		return streamToList.getList();
	}

	@Test
	public void testInitEmptyState() throws ExecutionException, InterruptedException {
		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		dataStorageTreeMap.getSortedOutput(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(Collections.emptyList(), toList(callback.get()));
	}

	@Test
	public void testInitNonEmptyState() throws ExecutionException, InterruptedException {
		final List<KeyValue<Integer, Set<String>>> data = asList(newKeyValue(1, "a"), newKeyValue(2, "b", "bb"));
		for (KeyValue<Integer, Set<String>> keyValue : data) state.put(keyValue.getKey(), keyValue.getValue());

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		dataStorageTreeMap.getSortedOutput(ALWAYS_TRUE, callback);

		eventloop.run();
		assertEquals(data, toList(callback.get()));
	}

	@Test
	public void testGetSortedStreamPredicate() throws ExecutionException, InterruptedException {
		final List<KeyValue<Integer, Set<String>>> data = asList(newKeyValue(0, "a"), newKeyValue(1, "b"), newKeyValue(2, "c"), newKeyValue(3, "d"));
		for (KeyValue<Integer, Set<String>> keyValue : data) state.put(keyValue.getKey(), keyValue.getValue());

		final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
		dataStorageTreeMap.getSortedOutput(Predicates.in(asList(0, 3)), callback);

		eventloop.run();
		assertEquals(asList(data.get(0), data.get(3)), toList(callback.get()));
	}

	@Test
	public void testSynchronize() throws ExecutionException, InterruptedException {
		final KeyValue<Integer, Set<String>> dataId2 = newKeyValue(2, "b");
		state.put(dataId2.getKey(), dataId2.getValue());

		final KeyValue<Integer, Set<String>> dataId1 = newKeyValue(1, "a");
		setUpTreeStorage();

		{
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
			dataStorageTreeMap.getSortedOutput(ALWAYS_TRUE, callback);

			eventloop.run();
			assertEquals(singletonList(dataId2), toList(callback.get()));
		}
		{
			final CompletionCallbackFuture syncCallback = CompletionCallbackFuture.create();
			dataStorageTreeMap.getSortedInput(new ForwardingResultCallback<StreamConsumer<KeyValue<Integer, Set<String>>>>(syncCallback) {
				@Override
				protected void onResult(StreamConsumer<KeyValue<Integer, Set<String>>> consumer) {
					ofIterable(eventloop, singleton(dataId1)).streamTo(listenableConsumer(consumer, syncCallback));
				}
			});

			eventloop.run();
			syncCallback.get();
		}
		{
			final ResultCallbackFuture<StreamProducer<KeyValue<Integer, Set<String>>>> callback = ResultCallbackFuture.create();
			dataStorageTreeMap.getSortedOutput(ALWAYS_TRUE, callback);

			eventloop.run();
			assertEquals(asList(dataId1, dataId2), toList(callback.get()));
		}
	}

	@Test
	public void testAccessors() {
		assertFalse(dataStorageTreeMap.hasKey(1));
		assertNull(dataStorageTreeMap.put(1, ImmutableSet.of("A", "B", "C")));
		assertTrue(dataStorageTreeMap.hasKey(1));

		assertNotNull(dataStorageTreeMap.put(1, ImmutableSet.of("A", "B", "E")));
		assertEquals(1, dataStorageTreeMap.size());

		assertEquals(ImmutableSet.of("A", "B", "E"), dataStorageTreeMap.get(1));
		assertNull(dataStorageTreeMap.get(2));
		assertEquals(1, dataStorageTreeMap.size());
	}
}