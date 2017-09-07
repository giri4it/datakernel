package io.datakernel.storage;

import com.google.common.collect.Ordering;
import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;
import io.datakernel.stream.processor.StreamSplitter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import static io.datakernel.stream.StreamConsumers.listenableConsumer;

public final class StorageNodeListenable<K extends Comparable<K>, V, A> implements StorageNode<K, V> {
	private final Eventloop eventloop;
	private final StorageNode<K, V> storageNode;

	private StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer;

	// null -> no requests, not null -> process one request
	private List<SettableStage<StreamProducer<KeyValue<K, V>>>> producerStages = null;
	private List<SettableStage<StreamConsumer<KeyValue<K, V>>>> consumerStages = null;

	// asserts only,  self check in runtime, improve readability
	private int getSortedProducerCounter;
	private int getSortedConsumerCounter;

	public StorageNodeListenable(Eventloop eventloop, StorageNode<K, V> storageNode,
	                             StreamReducers.Reducer<K, KeyValue<K, V>, KeyValue<K, V>, A> reducer) {
		this.eventloop = eventloop;
		this.storageNode = storageNode;
		this.reducer = reducer;
	}

	@Override
	public CompletionStage<StreamProducer<KeyValue<K, V>>> getSortedOutput(java.util.function.Predicate<K> predicate) {
		assert eventloop.inEventloopThread();

		final SettableStage<StreamProducer<KeyValue<K, V>>> stage = SettableStage.create();
		if (producerStages != null) {
			producerStages.add(stage);
			return stage;
		}

		producerStages = new ArrayList<>();
		producerStages.add(stage);
		eventloop.post(() -> doGetSortedStreamProducer(predicate));
		return stage;
	}

	private void doGetSortedStreamProducer(final Predicate<K> predicate) {
		//noinspection AssertWithSideEffects
		assert ++getSortedProducerCounter == 1;

		storageNode.getSortedOutput(predicate).whenComplete((producer, throwable) -> {
			final List<SettableStage<StreamProducer<KeyValue<K, V>>>> stages = producerStages;
			producerStages = new ArrayList<>();

			if (throwable == null) {
				final StreamSplitter<KeyValue<K, V>> splitter = StreamSplitter.create(eventloop);
				final StreamConsumers.StreamConsumerListenable<KeyValue<K, V>> downstreamConsumer = listenableConsumer(splitter.getInput());
				producer.streamTo(downstreamConsumer);
				downstreamConsumer.getStage().whenComplete((aVoid, throwable1) -> {
					//noinspection AssertWithSideEffects
					assert --getSortedProducerCounter == 0;

					if (producerStages.size() != 0) {
						eventloop.post(() -> doGetSortedStreamProducer(predicate));
					} else {
						producerStages = null;
					}
				});

				stages.forEach(stage -> stage.setResult(splitter.newOutput()));
			} else {
				stages.forEach(stage -> stage.setError(throwable));
			}
		});
	}

	@Override
	public CompletionStage<StreamConsumer<KeyValue<K, V>>> getSortedInput() {
		assert eventloop.inEventloopThread();

		final SettableStage<StreamConsumer<KeyValue<K, V>>> stage = SettableStage.create();
		if (consumerStages != null) {
			consumerStages.add(stage);
			return stage;
		}

		consumerStages = new ArrayList<>();
		consumerStages.add(stage);
		eventloop.post(this::doGetSortedStreamConsumer);
		return stage;
	}

	private void doGetSortedStreamConsumer() {
		//noinspection AssertWithSideEffects
		assert ++getSortedConsumerCounter == 1;

		storageNode.getSortedInput().whenComplete((consumer, throwable) -> {
			final List<SettableStage<StreamConsumer<KeyValue<K, V>>>> stages = consumerStages;
			consumerStages = new ArrayList<>();

			if (throwable == null) {
				final StreamReducer<K, KeyValue<K, V>, A> streamReducer = StreamReducer.create(eventloop, Ordering.<K>natural());
				final StreamConsumers.StreamConsumerListenable<KeyValue<K, V>> downstreamConsumer = listenableConsumer(consumer);
				streamReducer.getOutput().streamTo(downstreamConsumer);
				downstreamConsumer.getStage().whenComplete((aVoid, throwable1) -> {
					//noinspection AssertWithSideEffects
					assert --getSortedConsumerCounter == 0;

					if (consumerStages.size() != 0) {
						eventloop.post(StorageNodeListenable.this::doGetSortedStreamConsumer);
					} else {
						consumerStages = null;
					}
				});

				stages.forEach(stage -> stage.setResult(streamReducer.newInput(KeyValue::getKey, reducer)));
			} else {
				stages.forEach(stage -> stage.setError(throwable));
			}
		});
	}

}
