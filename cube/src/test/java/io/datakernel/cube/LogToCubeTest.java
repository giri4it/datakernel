/*
 * Copyright (C) 2015 SoftIndex LLC.
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

import com.google.common.collect.ImmutableMap;
import io.datakernel.aggregation_db.*;
import io.datakernel.aggregation_db.fieldtype.FieldType;
import io.datakernel.aggregation_db.fieldtype.FieldTypeLong;
import io.datakernel.aggregation_db.keytype.KeyType;
import io.datakernel.aggregation_db.keytype.KeyTypeInt;
import io.datakernel.async.AsyncCallbacks;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.cube.bean.TestPubRequest;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.logfs.LocalFsLogFileSystem;
import io.datakernel.logfs.LogManager;
import io.datakernel.logfs.LogManagerImpl;
import io.datakernel.logfs.LogToCubeRunner;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.cube.TestUtils.deleteRecursivelyQuietly;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogToCubeTest {
	private static final Logger logger = LoggerFactory.getLogger(LogToCubeTest.class);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	public static Cube newCube(Eventloop eventloop, DefiningClassLoader classLoader, CubeMetadataStorage cubeMetadataStorage,
	                           AggregationMetadataStorage aggregationMetadataStorage, AggregationChunkStorage aggregationChunkStorage,
	                           AggregationStructure aggregationStructure) {
		return new Cube(eventloop, classLoader, cubeMetadataStorage, aggregationMetadataStorage, aggregationChunkStorage,
				aggregationStructure, 1_000_000, 1_000_000);
	}

	public static AggregationStructure getStructure(DefiningClassLoader classLoader) {
		return new AggregationStructure(classLoader,
				ImmutableMap.<String, KeyType>builder()
						.put("pub", new KeyTypeInt())
						.put("adv", new KeyTypeInt())
						.build(),
				ImmutableMap.<String, FieldType>builder()
						.put("pubRequests", new FieldTypeLong())
						.put("advRequests", new FieldTypeLong())
						.build());
	}

	@Test
	public void testStubStorage() throws Exception {
		DefiningClassLoader classLoader = new DefiningClassLoader();
		NioEventloop eventloop = new NioEventloop();
		AggregationMetadataStorageStub aggregationMetadataStorage = new AggregationMetadataStorageStub();
		AggregationChunkStorageStub aggregationStorage = new AggregationChunkStorageStub(eventloop, classLoader);
		AggregationStructure structure = getStructure(classLoader);
		LogToCubeMetadataStorageStub logToCubeMetadataStorageStub = new LogToCubeMetadataStorageStub(aggregationMetadataStorage);
		Cube cube = newCube(eventloop, classLoader, logToCubeMetadataStorageStub, aggregationMetadataStorage, aggregationStorage, structure);
		cube.addAggregation(new AggregationMetadata("pub", asList("pub"), asList("pubRequests")));
		cube.addAggregation(new AggregationMetadata("adv", asList("adv"), asList("advRequests")));

		ExecutorService executor = Executors.newCachedThreadPool();
		Path dir = temporaryFolder.newFolder().toPath();
		deleteRecursivelyQuietly(dir);
		LocalFsLogFileSystem fileSystem = new LocalFsLogFileSystem(eventloop, executor, dir);
		BufferSerializer<TestPubRequest> bufferSerializer = SerializerBuilder
				.newDefaultInstance(classLoader)
				.create(TestPubRequest.class);

		LogManager<TestPubRequest> logManager = new LogManagerImpl<>(eventloop, fileSystem, bufferSerializer);

		LogToCubeRunner<TestPubRequest> logToCubeRunner = new LogToCubeRunner<>(eventloop, cube, logManager, TestAggregatorSplitter.factory(),
				"testlog", asList("partitionA"), logToCubeMetadataStorageStub);

		new StreamProducers.OfIterator<>(eventloop, asList(
				new TestPubRequest(1000, 1, asList(new TestPubRequest.TestAdvRequest(10))),
				new TestPubRequest(1001, 2, asList(new TestPubRequest.TestAdvRequest(10), new TestPubRequest.TestAdvRequest(20))),
				new TestPubRequest(1002, 1, asList(new TestPubRequest.TestAdvRequest(30))),
				new TestPubRequest(1002, 2, Arrays.<TestPubRequest.TestAdvRequest>asList())).iterator())
				.streamTo(logManager.consumer("partitionA"));

		eventloop.run();

		logToCubeRunner.processLog(AsyncCallbacks.ignoreCompletionCallback());

		eventloop.run();

		cube.loadChunks(ignoreCompletionCallback());

		eventloop.run();

		StreamConsumers.ToList<TestAdvResult> consumerToList = StreamConsumers.toList(eventloop);
		cube.query(TestAdvResult.class, new AggregationQuery(asList("adv"), asList("advRequests")))
				.streamTo(consumerToList);
		eventloop.run();

		List<TestAdvResult> actualResults = consumerToList.getList();
		List<TestAdvResult> expectedResults = asList(new TestAdvResult(10, 2), new TestAdvResult(20, 1), new TestAdvResult(30, 1));

		System.out.println(consumerToList.getList());

		assertEquals(expectedResults, actualResults);
	}

	public static final class TestPubResult {
		public int pub;
		public long pubRequests;

		@Override
		public String toString() {
			return "TestResult{pub=" + pub + ", pubRequests=" + pubRequests + '}';
		}
	}

	public static final class TestAdvResult {
		public int adv;
		public long advRequests;

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
