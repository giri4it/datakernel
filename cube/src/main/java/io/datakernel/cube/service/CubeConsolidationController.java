package io.datakernel.cube.service;

import io.datakernel.aggregation.Aggregation;
import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncFunction;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.cube.Cube;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.*;
import io.datakernel.logfs.ot.LogDiff;
import io.datakernel.logfs.ot.LogOTState;
import io.datakernel.ot.OTStateManager;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Supplier;

import static io.datakernel.async.AsyncCallable.sharedCall;
import static io.datakernel.jmx.ValueStats.SMOOTHING_WINDOW_5_MINUTES;
import static java.util.stream.Collectors.toSet;

public final class CubeConsolidationController implements EventloopJmxMBeanEx {
	public static final Supplier<AsyncFunction<Aggregation, AggregationDiff>> DEFAULT_STRATEGY = new Supplier<AsyncFunction<Aggregation, AggregationDiff>>() {
		private boolean hotSegment = false;

		@Override
		public AsyncFunction<Aggregation, AggregationDiff> get() {
			return (hotSegment = !hotSegment) ? Aggregation::consolidateHotSegment : Aggregation::consolidateMinKey;
		}
	};
	public static final double DEFAULT_SMOOTHING_WINDOW = SMOOTHING_WINDOW_5_MINUTES;

	private final Logger logger = LoggerFactory.getLogger(CubeConsolidationController.class);
	private final Eventloop eventloop;
	private final Cube cube;
	private final OTStateManager<Integer, LogDiff<CubeDiff>> stateManager;
	private final AggregationChunkStorage aggregationChunkStorage;
	private final Stopwatch sw = Stopwatch.createUnstarted();

	private final Supplier<AsyncFunction<Aggregation, AggregationDiff>> strategy;

	private final StageStats stageConsolidate = StageStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final StageStats stageConsolidateImpl = StageStats.create(DEFAULT_SMOOTHING_WINDOW);

	private final ValueStats removedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats removedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunks = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);
	private final ValueStats addedChunksRecords = ValueStats.create(DEFAULT_SMOOTHING_WINDOW);

	CubeConsolidationController(Eventloop eventloop, Cube cube, OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                            AggregationChunkStorage aggregationChunkStorage,
	                            Supplier<AsyncFunction<Aggregation, AggregationDiff>> strategy) {
		this.eventloop = eventloop;
		this.cube = cube;
		this.stateManager = stateManager;
		this.aggregationChunkStorage = aggregationChunkStorage;
		this.strategy = strategy;
	}

	public static CubeConsolidationController create(Eventloop eventloop,
	                                                 OTStateManager<Integer, LogDiff<CubeDiff>> stateManager,
	                                                 AggregationChunkStorage aggregationChunkStorage) {
		LogOTState<CubeDiff> logState = (LogOTState<CubeDiff>) stateManager.getState();
		Cube cube = (Cube) logState.getDataState();
		return new CubeConsolidationController(eventloop, cube, stateManager, aggregationChunkStorage, DEFAULT_STRATEGY);
	}

	public CubeConsolidationController withStrategy(Supplier<AsyncFunction<Aggregation, AggregationDiff>> strategy) {
		return new CubeConsolidationController(eventloop, cube, stateManager, aggregationChunkStorage, strategy);
	}

	private final AsyncCallable<Void> consolidate = sharedCall(this::doConsolidate);

	public Stage<Void> consolidate() {
		return consolidate.call();
	}

	Stage<Void> doConsolidate() {
		sw.reset().start();
		return stateManager.pull()
				.thenCompose($ -> stateManager.getAlgorithms().mergeHeadsAndPush())
				.thenCompose(mergeId -> stateManager.pull(mergeId))
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> cube.consolidate(strategy.get()).whenComplete(stageConsolidateImpl.recordStats()))
				.whenComplete(Stages.onResult(this::cubeDiffJmx))
				.whenComplete(this::logCubeDiff)
				.thenCompose(this::tryPushConsolidation)
				.whenComplete(this::logResult)
				.thenAccept($ -> {})
				.whenComplete(stageConsolidate.recordStats());

	}

	private void cubeDiffJmx(CubeDiff cubeDiff) {
		long curAddedChunks = 0;
		long curAddedChunksRecords = 0;
		long curRemovedChunks = 0;
		long curRemovedChunksRecords = 0;

		for (String key : cubeDiff.keySet()) {
			AggregationDiff aggregationDiff = cubeDiff.get(key);
			curAddedChunks += aggregationDiff.getAddedChunks().size();
			for (AggregationChunk aggregationChunk : aggregationDiff.getAddedChunks()) {
				curAddedChunksRecords += aggregationChunk.getCount();
			}

			curRemovedChunks += aggregationDiff.getRemovedChunks().size();
			for (AggregationChunk aggregationChunk : aggregationDiff.getRemovedChunks()) {
				curRemovedChunksRecords += aggregationChunk.getCount();
			}
		}

		addedChunks.recordValue(curAddedChunks);
		addedChunksRecords.recordValue(curAddedChunksRecords);
		removedChunks.recordValue(curRemovedChunks);
		removedChunksRecords.recordValue(curRemovedChunksRecords);
	}

	private Stage<Void> tryPushConsolidation(CubeDiff cubeDiff) {
		if (cubeDiff.isEmpty()) return Stage.of(null);

		stateManager.add(LogDiff.forCurrentPosition(cubeDiff));
		return stateManager.pull()
				.thenCompose($ -> stateManager.getAlgorithms().mergeHeadsAndPush())
				.thenCompose(mergeId -> stateManager.pull(mergeId))
				.thenCompose($ -> stateManager.pull())
				.thenCompose($ -> stateManager.commit())
				.thenCompose($ -> stateManager.push())
				.thenCompose($ -> aggregationChunkStorage.finish(addedChunks(cubeDiff)));
	}

	private static Set<Long> addedChunks(CubeDiff cubeDiff) {
		return cubeDiff.addedChunks().collect(toSet());
	}

	private void logCubeDiff(CubeDiff cubeDiff, Throwable throwable) {
		if (throwable != null) logger.warn("Consolidation failed", throwable);
		else if (cubeDiff.isEmpty()) logger.info("Previous consolidation did not merge any chunks");
		else logger.info("Consolidation finished. Launching consolidation task again.");
	}

	private <T> void logResult(T value, Throwable throwable) {
		if (throwable == null) logger.info("Consolidator finish in {}", sw.stop());
		else logger.error("Consolidator error in {}", sw.stop(), throwable);
	}

	@JmxAttribute
	public ValueStats getRemovedChunks() {
		return removedChunks;
	}

	@JmxAttribute
	public ValueStats getAddedChunks() {
		return addedChunks;
	}

	@JmxAttribute
	public ValueStats getRemovedChunksRecords() {
		return removedChunksRecords;
	}

	@JmxAttribute
	public ValueStats getAddedChunksRecords() {
		return addedChunksRecords;
	}

	@JmxAttribute
	public StageStats getStageConsolidate() {
		return stageConsolidate;
	}

	@JmxAttribute
	public StageStats getStageConsolidateImpl() {
		return stageConsolidateImpl;
	}

	@JmxOperation
	public void consolidateNow() {
		consolidate();
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}
}