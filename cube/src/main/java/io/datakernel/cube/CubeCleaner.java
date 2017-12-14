package io.datakernel.cube;

import io.datakernel.aggregation.LocalFsChunkStorage;
import io.datakernel.async.AsyncPredicate;
import io.datakernel.async.Stages;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.ot.OTAlgorithms.*;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemoteSql;
import io.datakernel.ot.OTSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static io.datakernel.async.Stages.callable;
import static io.datakernel.ot.OTAlgorithms.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class CubeCleaner {
	private final Logger logger = LoggerFactory.getLogger(CubeCleaner.class);
	private final Eventloop eventloop;
	private final OTRemoteSql<LogDiff<CubeDiff>> otRemote;
	private final Comparator<Integer> comparator;
	private final OTSystem<LogDiff<CubeDiff>> otSystem;
	private final LocalFsChunkStorage storage;
	private final long chunksCleanupDelay;

	public CubeCleaner(Eventloop eventloop, OTRemoteSql<LogDiff<CubeDiff>> otRemote, Comparator<Integer> comparator,
	                   OTSystem<LogDiff<CubeDiff>> otSystem, LocalFsChunkStorage storage, long chunksCleanupDelay) {
		this.eventloop = eventloop;
		this.otRemote = otRemote;
		this.comparator = comparator;
		this.otSystem = otSystem;
		this.storage = storage;
		this.chunksCleanupDelay = chunksCleanupDelay;
	}

	// TODO: improve min(comparator)
	public CompletionStage<Void> cleanup(Set<Integer> parentCandidates) {
		logger.info("Parent candidates: {}", parentCandidates);

		return otRemote.getHeads()
				// check states
				.thenCompose(heads -> Stages.runSequence(heads.stream()
						.map(head -> callable(() -> loadAllChanges(otRemote, comparator, otSystem, head)))
						.collect(toList())))
				.thenCompose(aVoid -> findRootNodes(otRemote, comparator, parentCandidates)
						.whenComplete(Stages.onResult(rootNodes -> logger.info("Root nodes: {}", rootNodes)))
						.thenCompose(rootNodes -> rootNodes.isEmpty() ? Stages.of(null)
								: findRootNodes(otRemote, comparator, rootNodes)
								.thenCompose(ks -> ks.stream().min(comparator).map(checkpointNode -> {
									logger.info("Checkpoint node: {}", checkpointNode);

									return loadAllChanges(otRemote, comparator, otSystem, checkpointNode)
											.thenCompose(changes -> otRemote.saveSnapshot(checkpointNode, changes)
													.thenCompose($ -> otRemote.cleanup(checkpointNode))
													.thenCompose($ -> otRemote.getHeads())
													.thenCompose(heads -> loadPathLogDiffs(checkpointNode, heads))
													.thenApply(logDiffs -> concat(changes, logDiffs))
													.thenApply(CubeCleaner::addedChunks)
													.thenCompose(this::compareChunks)
													.thenCompose(this::cleanupChunks));
								}).orElse(Stages.of(null)))));

	}

	private CompletionStage<Set<Long>> compareChunks(Set<Long> cubeChunks) {
		return storage.list(s -> true).thenCompose(storageChunks -> storageChunks.containsAll(cubeChunks)
				? Stages.of(cubeChunks)
				: Stages.ofException(new IllegalStateException("Missed chunks from storage: " +
				new HashSet<>(cubeChunks).removeAll(storageChunks))));
	}

	private CompletionStage<Collection<LogDiff<CubeDiff>>> loadPathLogDiffs(Integer parent, Collection<Integer> heads) {
		final List<CompletionStage<List<LogDiff<CubeDiff>>>> tasks = heads.stream()
				.map(head -> findParent(otRemote, comparator, head, parent, compareId(parent))
						.thenApply(FindResult::getParentToChild))
				.collect(toList());

		return Stages.reduce(new ArrayList<>(), (accumulator, item, index) -> accumulator.addAll(item), tasks);
	}

	private static <K> AsyncPredicate<OTCommit<K, LogDiff<CubeDiff>>> compareId(K snapshotId) {
		return commit -> Stages.of(snapshotId.equals(commit.getId()));
	}

	private static Stream<LogDiff<CubeDiff>> concat(Collection<LogDiff<CubeDiff>> changes,
	                                                Collection<LogDiff<CubeDiff>> logDiffs) {
		return Stream.concat(changes.stream(), logDiffs.stream());
	}

	private static Set<Long> addedChunks(Stream<LogDiff<CubeDiff>> stream) {
		return stream.flatMap(LogDiff::diffs).flatMap(CubeDiff::addedChunks).collect(toSet());
	}

	private CompletionStage<Void> cleanupChunks(Set<Long> chunks) {
		return storage.cleanupBeforeTimestamp(chunks, eventloop.currentTimeMillis() - chunksCleanupDelay);
	}

}
