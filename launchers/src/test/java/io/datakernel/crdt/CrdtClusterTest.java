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

package io.datakernel.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.async.Promises;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.json.JsonUtils;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.HttpRequest;
import io.datakernel.jmx.PromiseStats;
import io.datakernel.launchers.crdt.*;
import io.datakernel.launchers.http.JmxHttpModule;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.DatakernelRunner.SkipEventloopRun;
import io.datakernel.stream.processor.Manual;
import io.datakernel.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.config.ConfigConverters.ofExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.http.HttpMethod.PUT;
import static io.datakernel.serializer.asm.BufferSerializers.INT_SERIALIZER;
import static io.datakernel.serializer.asm.BufferSerializers.JAVA_UTF8_SERIALIZER;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Manual("manual demos")
@RunWith(DatakernelRunner.class)
public final class CrdtClusterTest {
	static {
		TestUtils.enableLogging();
	}

	static class BusinessLogicModule extends AbstractModule {
		@Provides
		CrdtDescriptor<String, Integer> provideDescriptor() {
			return new CrdtDescriptor<>(Math::max, new CrdtDataSerializer<>(JAVA_UTF8_SERIALIZER, INT_SERIALIZER), STRING_CODEC, INT_CODEC);
		}

		@Provides
		@Singleton
		ExecutorService provideExecutor(Config config) {
			return config.get(ofExecutor(), "crdt.local.executor");
		}

		@Provides
		@Singleton
		FsClient provideFsClient(Eventloop eventloop, ExecutorService executor, Config config) {
			return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "crdt.local.path"));
		}
	}

	static class TestNodeLauncher extends CrdtNodeLauncher<String, Integer> {
		private final int jmxHttpPort;
		private final Config config;

		public TestNodeLauncher(int jmxHttpPort, Config config) {
			this.jmxHttpPort = jmxHttpPort;
			this.config = config;
		}

		@Override
		protected Collection<Module> getOverrideModules() {
			return singletonList(ConfigModule.create(config));
		}

		@Override
		protected CrdtNodeLogicModule<String, Integer> getLogicModule() {
			return new CrdtNodeLogicModule<String, Integer>() {};
		}

		@Override
		protected Collection<Module> getBusinessLogicModules() {
			return asList(
					JmxHttpModule.defaultInstance().withServer(jmxHttpPort),
					new CrdtHttpModule<String, Integer>() {},
					new BusinessLogicModule()
			);
		}
	}

	@Test
	@SkipEventloopRun
	public void startFirst() throws Exception {
		new TestNodeLauncher(6565, Config.create()
				.with("crdt.http.listenAddresses", "localhost:7000")
				.with("crdt.server.listenAddresses", "localhost:8000")
				.with("crdt.cluster.server.listenAddresses", "localhost:9000")
				.with("crdt.local.path", "/tmp/TESTS/crdt")
				.with("crdt.cluster.localPartitionId", "first")
				.with("crdt.cluster.replicationCount", "2")
				.with("crdt.cluster.partitions.second", "localhost:8001")
//				.with("crdt.cluster.partitions.file", "localhost:8002")
		).launch(false, new String[0]);
	}

	@Test
	@SkipEventloopRun
	public void startSecond() throws Exception {
		new TestNodeLauncher(6566, Config.create()
				.with("crdt.http.listenAddresses", "localhost:7001")
				.with("crdt.server.listenAddresses", "localhost:8001")
				.with("crdt.cluster.server.listenAddresses", "localhost:9001")
				.with("crdt.local.path", "/tmp/TESTS/crdt")
				.with("crdt.cluster.localPartitionId", "second")
				.with("crdt.cluster.replicationCount", "2")
				.with("crdt.cluster.partitions.first", "localhost:8000")
//				.with("crdt.cluster.partitions.file", "localhost:8002")
		).launch(false, new String[0]);
	}

	@Test
	@SkipEventloopRun
	public void startFileServer() throws Exception {
		new CrdtFileServerLauncher<String, Integer>() {

			@Override
			protected CrdtFileServerLogicModule<String, Integer> getLogicModule() {
				return new CrdtFileServerLogicModule<String, Integer>() {};
			}

			@Override
			protected Collection<Module> getOverrideModules() {
				return singletonList(ConfigModule.create(() ->
						Config.create()
								.with("crdt.localPath", "/tmp/TESTS/fileServer")
								.with("crdt.server.listenAddresses", "localhost:8002")));
			}

			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new AbstractModule() {
					@Provides
					@Singleton
					CrdtDescriptor<String, Integer> provideDescriptor() {
						return new CrdtDescriptor<>(Math::max, new CrdtDataSerializer<>(JAVA_UTF8_SERIALIZER, INT_SERIALIZER), STRING_CODEC, INT_CODEC);
					}
				});
			}
		}.launch(false, new String[0]);
	}

	@Test
	public void uploadWithHTTP() {
		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());

		PromiseStats uploadStat = PromiseStats.create(Duration.ofSeconds(5));

		StructuredCodec<CrdtData<String, Integer>> codec = record(CrdtData::new,
				CrdtData::getKey, STRING_CODEC,
				CrdtData::getState, INT_CODEC);

		Promises.runSequence(IntStream.range(0, 1_000_000)
				.mapToObj(i ->
						client.request(HttpRequest.of(PUT, "http://127.0.0.1:7000")
								.withBody(JsonUtils.toJson(codec, new CrdtData<>("value_" + i, i)).getBytes(UTF_8)))))
				.whenException(System.err::println)
				.whenComplete(uploadStat.recordStats())
				.whenComplete(assertComplete($ -> System.out.println(uploadStat)));

		// RemoteCrdtClient<String, Integer> client = RemoteCrdtClient.create(eventloop, ADDRESS, CRDT_DATA_SERIALIZER);
		//
		// StreamProducer.ofStream(IntStream.range(0, 1000000)
		// 		.mapToObj(i -> new CrdtData<>("key_" + i, i))).streamTo(client.uploadStream())
		// 		.getEndOfStream()
		// 		.thenRun(() -> System.out.println("finished"));
	}

	@Test
	public void uploadWithStreams() {
		RemoteCrdtClient<String, Integer> client = RemoteCrdtClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress("localhost", 9000), JAVA_UTF8_SERIALIZER, INT_SERIALIZER);

		PromiseStats uploadStat = PromiseStats.create(Duration.ofSeconds(5));

		StreamSupplier.ofStream(IntStream.range(0, 1000000)
				.mapToObj(i -> new CrdtData<>("value_" + i, i))).streamTo(client.uploader())
				.whenComplete(uploadStat.recordStats())
				.whenComplete(assertComplete($ -> {
					System.out.println(uploadStat);
					System.out.println("finished");
				}));
	}

	@Test
	public void downloadStuff() {
		RemoteCrdtClient<String, Integer> client = RemoteCrdtClient.create(Eventloop.getCurrentEventloop(), new InetSocketAddress(9001), JAVA_UTF8_SERIALIZER, INT_SERIALIZER);

		client.download()
				.getStream()
				.streamTo(StreamConsumer.of(System.out::println))
				.whenComplete(assertComplete($ -> System.out.println("finished")));
	}
}