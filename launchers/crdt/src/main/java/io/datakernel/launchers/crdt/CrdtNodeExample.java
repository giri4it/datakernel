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

package io.datakernel.launchers.crdt;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static java.lang.Boolean.parseBoolean;
import static java.util.Collections.singletonList;

public final class CrdtNodeExample {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";

	static class BusinessLogicModule extends AbstractModule {
		@Provides
		CrdtDescriptor<String, Integer> provideDescriptor() {
			return new CrdtDescriptor<>(Math::max, new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER), STRING_CODEC, INT_CODEC);
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

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CrdtNodeLauncher<String, Integer>() {

			@Override
			protected CrdtNodeLogicModule<String, Integer> getLogicModule() {
				return new CrdtNodeLogicModule<String, Integer>() {};
			}

			@Override
			protected Collection<Module> getOverrideModules() {
				return singletonList(
						ConfigModule.create(() ->
								Config.create()
										.with("crdt.http.listenAddresses", "localhost:8080")
										.with("crdt.server.listenAddresses", "localhost:9090")
										.with("crdt.local.path", "/tmp/TESTS/crdt")
										.with("crdt.cluster.localPartitionId", "local")
										.with("crdt.cluster.partitions.noop", "localhost:9091")
										.override(ofProperties(PROPERTIES_FILE, true))
										.override(ofProperties(System.getProperties()).getChild("config")))
								.printEffectiveConfig());
			}

			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new BusinessLogicModule());
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
