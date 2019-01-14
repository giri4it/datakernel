package io.datakernel.cube.service;

import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.async.AsyncSupplier;
import io.datakernel.async.Promise;
import io.datakernel.async.Promises;
import io.datakernel.cube.CubeDiffScheme;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.EventloopJmxMBeanEx;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.ot.DiffsReducer;
import io.datakernel.ot.OTAlgorithms;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRepositoryEx;
import io.datakernel.ot.exceptions.OTException;
import io.datakernel.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.async.AsyncSuppliers.reuse;
import static io.datakernel.cube.Utils.chunksInDiffs;
import static io.datakernel.util.CollectionUtils.toLimitedString;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.LogUtils.Level.TRACE;
import static io.datakernel.util.LogUtils.thisMethod;
import static io.datakernel.util.LogUtils.toLogger;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

public final class CubeCleanerController<K, D, C> implements EventloopJmxMBeanEx {
	private static final Logger logger = LoggerFactory.getLogger(CubeCleanerController.class);

	public static final Duration DEFAULT_CHUNKS_CLEANUP_DELAY = Duration.ofMinutes(1);
	public static final int DEFAULT_SNAPSHOTS_COUNT = 1;
	public static final Duration DEFAULT_SMOOTHING_WINDOW = Duration.ofMinutes(5);

	private final Eventloop eventloop;

	private final OTAlgorithms<K, D> algorithms;
	private final OTRepositoryEx<K, D> repository;
	private final RemoteFsChunkStorage<C> chunksStorage;

	private final CubeDiffScheme<D> cubeDiffScheme;

	private Duration freezeTimeout;

	private Duration chunksCleanupDelay = DEFAULT_CHUNKS_CLEANUP_DELAY;
	private int extraSnapshotsCount = DEFAULT_SNAPSHOTS_COUNT;

	private final PromiseStats promiseCleanup = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupCollectRequiredChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupRepository = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final PromiseStats promiseCleanupChunks = PromiseStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeCleanerController(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			OTRepositoryEx<K, D> repository,
			RemoteFsChunkStorage<C> chunksStorage) {
		this.eventloop = eventloop;
		this.cubeDiffScheme = cubeDiffScheme;
		this.algorithms = algorithms;
		this.repository = repository;
		this.chunksStorage = chunksStorage;
	}

	public static <K, D, C> CubeCleanerController<K, D, C> create(Eventloop eventloop,
			CubeDiffScheme<D> cubeDiffScheme,
			OTAlgorithms<K, D> algorithms,
			RemoteFsChunkStorage<C> storage) {
		return new CubeCleanerController<>(eventloop, cubeDiffScheme, algorithms, (OTRepositoryEx<K, D>) algorithms.getRepository(), storage);
	}

	public CubeCleanerController<K, D, C> withChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
		return this;
	}

	public CubeCleanerController<K, D, C> withExtraSnapshotsCount(int extraSnapshotsCount) {
		this.extraSnapshotsCount = extraSnapshotsCount;
		return this;
	}

	public CubeCleanerController<K, D, C> withFreezeTimeout(Duration freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
		return this;
	}

	private static <K, D> Stream<D> commitToDiffs(OTCommit<K, D> commit) {
		return commit.getParents().values().stream().flatMap(Collection::stream);
	}

	private final AsyncSupplier<Void> cleanup = reuse(this::doCleanup);

	public Promise<Void> cleanup() {
		return cleanup.get();
	}

	Promise<Void> doCleanup() {
		return repository.getHeads()
				.thenCompose(heads -> {
					if (heads.isEmpty()) {
						logger.info("No heads found, skip cleanup");
						return Promise.complete();
					}
					return algorithms.excludeParents(heads)
							.thenCompose(filteredHeads -> findFrozenCut(filteredHeads, eventloop.currentInstant().minus(freezeTimeout)))
							.thenCompose(this::cleanupFrozenCut)
							.whenComplete(promiseCleanup.recordStats());
				})
				.whenComplete(toLogger(logger, thisMethod()));
	}

	Promise<Set<K>> findFrozenCut(Set<K> heads, Instant freezeTimestamp) {
		return algorithms.findCut(heads,
				commits -> commits.stream().allMatch(commit -> commit.getInstant().compareTo(freezeTimestamp) < 0))
				.whenComplete(toLogger(logger, thisMethod(), heads, freezeTimestamp));
	}

	Promise<Void> cleanupFrozenCut(Set<K> frozenCut) {
		return Promise.of(frozenCut)
				.thenCompose(algorithms::findAllCommonParents)
				.thenCompose(algorithms::findAnyCommonParent)
				.thenCompose(this::trySaveSnapshotAndCleanupChunks)
				.whenComplete(toLogger(logger, thisMethod(), frozenCut));
	}

	static class Tuple<K, D, C> {
		final Set<C> collectedChunks;
		final OTCommit<K, D> lastSnapshot;

		Tuple(Set<C> collectedChunks, OTCommit<K, D> lastSnapshot) {
			this.collectedChunks = collectedChunks;
			this.lastSnapshot = lastSnapshot;
		}
	}

	Promise<Void> trySaveSnapshotAndCleanupChunks(K checkpointNode) {
		return Promise.of(checkpointNode)
				.thenCompose(commitId -> algorithms.checkout(commitId)
						.thenComposeEx((checkpointDiffs, e) -> {
							if (e == null) {
								return repository.saveSnapshot(checkpointNode, checkpointDiffs)
										.thenCompose($ -> findSnapshot(singleton(checkpointNode), extraSnapshotsCount))
										.thenCompose(lastSnapshot -> {
											if (lastSnapshot.isPresent())
												return Promises.toTuple(Tuple::new,
														collectRequiredChunks(checkpointNode),
														repository.loadCommit(lastSnapshot.get()))
														.thenCompose(tuple ->
																cleanup(lastSnapshot.get(),
																		union(chunksInDiffs(cubeDiffScheme, checkpointDiffs), tuple.collectedChunks),
																		tuple.lastSnapshot.getInstant().minus(chunksCleanupDelay)));
											else {
												logger.info("Not enough snapshots, skip cleanup");
												return Promise.complete();
											}
										});
							} else if (e instanceof OTException) {
								logger.info("Failed to checkout, maybe snapshot is not currently present, skip cleanup");
								return Promise.complete();
							} else {
								return Promise.ofException(e);
							}
						}))
				.whenComplete(toLogger(logger, thisMethod(), checkpointNode));
	}

	Promise<Optional<K>> findSnapshot(Set<K> heads, int skipSnapshots) {
		return algorithms.findParent(heads, DiffsReducer.toVoid(),
				commit -> commit.getSnapshotHint() == Boolean.FALSE ?
						Promise.of(false) :
						commit.getSnapshotHint() == Boolean.TRUE ?
								Promise.of(true) :
								repository.hasSnapshot(commit.getId()))
				.async()
				.thenCompose(findResult -> {
					if (!findResult.isFound()) return Promise.of(Optional.empty());
					else if (skipSnapshots <= 0) return Promise.of(Optional.of(findResult.getCommit()));
					else if (findResult.getCommitParents().isEmpty()) return Promise.of(Optional.empty());
					else return findSnapshot(findResult.getCommitParents(), skipSnapshots - 1);
				});
	}

	private Promise<Set<C>> collectRequiredChunks(K checkpointNode) {
		return repository.getHeads()
				.thenCompose(heads ->
						algorithms.reduceEdges(heads, checkpointNode,
								DiffsReducer.of(
										new HashSet<>(),
										(Set<C> accumulatedChunks, List<D> diffs) ->
												union(accumulatedChunks, chunksInDiffs(cubeDiffScheme, diffs)),
										CollectionUtils::union))
								.whenComplete(promiseCleanupCollectRequiredChunks.recordStats()))
				.thenApply(accumulators -> accumulators.values().stream().flatMap(Collection::stream).collect(toSet()))
				.whenComplete(transform(Set::size,
						toLogger(logger, thisMethod(), checkpointNode)));
	}

	private Promise<Void> cleanup(K checkpointNode, Set<C> requiredChunks, Instant chunksCleanupTimestamp) {
		return chunksStorage.checkRequiredChunks(requiredChunks)
				.thenCompose($ -> repository.cleanup(checkpointNode)
						.whenComplete(promiseCleanupRepository.recordStats()))
				.thenCompose($ -> chunksStorage.cleanup(requiredChunks, chunksCleanupTimestamp)
						.whenComplete(promiseCleanupChunks.recordStats()))
				.whenComplete(logger.isTraceEnabled() ?
						toLogger(logger, TRACE, thisMethod(), checkpointNode, chunksCleanupTimestamp, requiredChunks) :
						toLogger(logger, thisMethod(), checkpointNode, chunksCleanupTimestamp, toLimitedString(requiredChunks, 6)));
	}

	@JmxAttribute
	public Duration getChunksCleanupDelay() {
		return chunksCleanupDelay;
	}

	@JmxAttribute
	public void setChunksCleanupDelay(Duration chunksCleanupDelay) {
		this.chunksCleanupDelay = chunksCleanupDelay;
	}

	@JmxAttribute
	public int getExtraSnapshotsCount() {
		return extraSnapshotsCount;
	}

	@JmxAttribute
	public void setExtraSnapshotsCount(int extraSnapshotsCount) {
		this.extraSnapshotsCount = extraSnapshotsCount;
	}

	@JmxAttribute
	public Duration getFreezeTimeout() {
		return freezeTimeout;
	}

	@JmxAttribute
	public void setFreezeTimeout(Duration freezeTimeout) {
		this.freezeTimeout = freezeTimeout;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanup() {
		return promiseCleanup;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupCollectRequiredChunks() {
		return promiseCleanupCollectRequiredChunks;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupRepository() {
		return promiseCleanupRepository;
	}

	@JmxAttribute
	public PromiseStats getPromiseCleanupChunks() {
		return promiseCleanupChunks;
	}

	@JmxOperation
	public void cleanupNow() {
		cleanup();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	private static <T, R> BiConsumer<R, Throwable> transform(Function<? super R, ? extends T> fn, BiConsumer<? super T, Throwable> toConsumer) {
		return (value, e) -> toConsumer.accept(value != null ? fn.apply(value) : null, e);
	}
}
