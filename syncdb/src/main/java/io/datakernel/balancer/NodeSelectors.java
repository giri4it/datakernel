package io.datakernel.balancer;

import io.datakernel.async.AsyncRunnable;
import io.datakernel.async.AsyncRunnables;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.storage.StorageNode;
import io.datakernel.stream.StreamProducers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Lists.newArrayList;

public class NodeSelectors {
	private NodeSelectors() {

	}

	public static <K, V> CompletionStage<List<StorageNode<K, V>>> selectAliveNodes(final Eventloop eventloop, List<? extends StorageNode<K, V>> nodes) {
		final List<AsyncRunnable> asyncRunnables = new ArrayList<>();
		final List<StorageNode<K, V>> aliveNodes = new ArrayList<>(Collections.<StorageNode<K, V>>nCopies(nodes.size(), null));

		for (int i = 0; i < nodes.size(); i++) {
			final StorageNode<K, V> node = nodes.get(i);
			final int finalI = i;
			asyncRunnables.add(() -> node.getSortedInput().whenComplete((consumer, throwable) -> {
				if (throwable == null) {
					aliveNodes.set(finalI, node);

					// close consumer
					StreamProducers.<StorageNode.KeyValue<K, V>>closing(eventloop).streamTo(consumer);
				}
			}).thenApply(keyValueStreamConsumer -> null));
		}

		return AsyncRunnables.runInParallel(eventloop, asyncRunnables).run()
				.thenApply(aVoid -> newArrayList(filter(aliveNodes, Objects::nonNull)));
	}
}
