/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.cube;

import io.datakernel.aggregation.AggregationChunkStorage;
import io.datakernel.aggregation.ChunkIdCodec;
import io.datakernel.aggregation.RemoteFsChunkStorage;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.cube.bean.TestPubRequest.TestAdvRequest;
import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.cube.ot.CubeDiffCodec;
import io.datakernel.cube.ot.CubeOT;
import io.datakernel.cube.service.CubeCleanerController;
import io.datakernel.etl.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.multilog.Multilog;
import io.datakernel.multilog.MultilogImpl;
import io.datakernel.ot.*;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.aggregation.AggregationPredicates.alwaysTrue;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofInt;
import static io.datakernel.aggregation.fieldtype.FieldTypes.ofLong;
import static io.datakernel.aggregation.measure.Measures.sum;
import static io.datakernel.async.TestUtils.await;
import static io.datakernel.cube.Cube.AggregationConfig.id;
import static io.datakernel.cube.TestUtils.initializeRepository;
import static io.datakernel.cube.TestUtils.runProcessLogs;
import static io.datakernel.multilog.LogNamingScheme.NAME_PARTITION_REMAINDER_SEQ;
import static io.datakernel.test.TestUtils.dataSource;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
@RunWith(DatakernelRunner.class)
public final class LogToCubeTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private Eventloop eventloop;
	private OTAlgorithms<Long, LogDiff<CubeDiff>> algorithms;
	private OTRepositoryMySql<LogDiff<CubeDiff>> repository;
	private Cube cube;
	private AggregationChunkStorage<Long> aggregationChunkStorage;
	private DefiningClassLoader classLoader;

	@Before
	public void setUp() throws Exception {
		DataSource dataSource = dataSource("test.properties");
		Path aggregationsDir = temporaryFolder.newFolder().toPath();
		ExecutorService executor = Executors.newCachedThreadPool();

		eventloop = Eventloop.getCurrentEventloop();

		classLoader = DefiningClassLoader.create();
		aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, executor, aggregationsDir));
		cube = Cube.create(eventloop, executor, classLoader, aggregationChunkStorage)
				.withDimension("pub", ofInt())
				.withDimension("adv", ofInt())
				.withMeasure("pubRequests", sum(ofLong()))
				.withMeasure("advRequests", sum(ofLong()))
				.withAggregation(id("pub").withDimensions("pub").withMeasures("pubRequests"))
				.withAggregation(id("adv").withDimensions("adv").withMeasures("advRequests"));

		OTSystem<LogDiff<CubeDiff>> otSystem = LogOT.createLogOT(CubeOT.createCubeOT());
		repository = OTRepositoryMySql.create(eventloop, executor, dataSource, otSystem, LogDiffCodec.create(CubeDiffCodec.create(cube)));
		algorithms = OTAlgorithms.create(eventloop, otSystem, repository);
		initializeRepository(repository);
	}

	@Test
	public void testStubStorage() throws Exception {
		Path logsDir = temporaryFolder.newFolder().toPath();
		List<TestAdvResult> expected = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		LogOTState<CubeDiff> cubeDiffLogOTState = LogOTState.create(cube);
		OTStateManager<Long, LogDiff<CubeDiff>> logCubeStateManager = OTStateManager.create(eventloop, algorithms, cubeDiffLogOTState);

		Multilog<TestPubRequest> multilog = MultilogImpl.create(eventloop,
				LocalFsClient.create(eventloop, newSingleThreadExecutor(), logsDir),
				SerializerBuilder.create(classLoader).build(TestPubRequest.class),
				NAME_PARTITION_REMAINDER_SEQ);

		LogOTProcessor<TestPubRequest, CubeDiff> logOTProcessor = LogOTProcessor.create(eventloop,
				multilog,
				new TestAggregatorSplitter(cube), // TestAggregatorSplitter.create(eventloop, cube),
				"testlog",
				asList("partitionA"),
				cubeDiffLogOTState);

		StreamSupplier<TestPubRequest> supplier = StreamSupplier.of(
				new TestPubRequest(1000, 1, asList(new TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestAdvRequest(10), new TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.asList()));

		await(supplier.streamTo(multilog.writer("partitionA")));
		await(logCubeStateManager.checkout());
		runProcessLogs(aggregationChunkStorage, logCubeStateManager, logOTProcessor);

		List<TestAdvResult> list = await(cube.queryRawStream(
				asList("adv"),
				asList("advRequests"),
				alwaysTrue(),
				TestAdvResult.class, classLoader)
				.toList());

		assertEquals(expected, list);
	}

	@Test
	public void testCleanupWithEmptyGraph() throws SQLException, IOException {
		repository.initialize();
		repository.truncateTables();

		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(eventloop,
				CubeDiffScheme.ofLogDiffs(), algorithms, (RemoteFsChunkStorage<Long>) aggregationChunkStorage)
				.withFreezeTimeout(Duration.ofMillis(100));
		await(cleanerController.cleanup());
	}

	@Test
	public void testCleanupWithNoSnapshot() throws SQLException, IOException {
		repository.initialize();
		repository.truncateTables();

		// Add one commit, but no snapshot
		Long id = await(repository.createCommitId());
		await(repository.push(OTCommit.ofRoot(id)));

		CubeCleanerController<Long, LogDiff<CubeDiff>, Long> cleanerController = CubeCleanerController.create(eventloop,
				CubeDiffScheme.ofLogDiffs(), algorithms, (RemoteFsChunkStorage<Long>) aggregationChunkStorage)
				.withFreezeTimeout(Duration.ofMillis(100));
		await(cleanerController.cleanup());
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

		@SuppressWarnings("unused")
		public TestAdvResult() {
		}

		public TestAdvResult(int adv, long advRequests) {
			this.adv = adv;
			this.advRequests = advRequests;
		}

		@Override
		public String toString() {
			return "TestAdvResult{adv=" + adv + ", advRequests=" + advRequests + '}';
		}

		@SuppressWarnings("RedundantIfStatement")
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestAdvResult that = (TestAdvResult) o;

			if (adv != that.adv) return false;
			if (advRequests != that.advRequests) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = adv;
			result = 31 * result + (int) (advRequests ^ (advRequests >>> 32));
			return result;
		}
	}
}
