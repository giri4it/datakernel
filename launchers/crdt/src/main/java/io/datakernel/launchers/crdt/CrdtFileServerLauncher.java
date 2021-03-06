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

import com.google.inject.*;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.crdt.CrdtServer;
import io.datakernel.crdt.local.FsCrdtClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggersModule;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.launchers.initializers.Initializers.ofAbstractServer;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public abstract class CrdtFileServerLauncher<K extends Comparable<K>, S> extends Launcher {
	public static final String PROPERTIES_FILE = "crdt-file-server.properties";

	@Inject
	CrdtServer<K, S> crdtServer;

	@Override
	protected Collection<Module> getModules() {
		return asList(override(getBaseModules()).with(getOverrideModules()),
				combine(getBusinessLogicModules()));
	}

	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	protected abstract CrdtFileServerLogicModule<K, S> getLogicModule();

	protected abstract Collection<Module> getBusinessLogicModules();

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				TriggersModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.override(ofProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				getLogicModule(),
				new AbstractModule() {

					@Provides
					@Singleton
					Eventloop provideEventloop() {
						return Eventloop.create();
					}

					@Provides
					@Singleton
					ExecutorService provideExecutor(Config config) {
						return config.get(ofExecutor(), "executor");
					}

					@Provides
					@Singleton
					LocalFsClient provideLocalFsClient(Eventloop eventloop, ExecutorService executor, Config config) {
						return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "crdt.localPath"));
					}
				}
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public abstract static class CrdtFileServerLogicModule<K extends Comparable<K>, S> extends AbstractModule {

		@Provides
		@Singleton
		CrdtServer<K, S> provideCrdtServer(Eventloop eventloop, FsCrdtClient<K, S> crdtClient, CrdtDescriptor<K, S> descriptor, Config config) {
			return CrdtServer.create(eventloop, crdtClient, descriptor.getSerializer())
					.initialize(ofAbstractServer(config.getChild("crdt.server")));
		}

		@Provides
		@Singleton
		FsCrdtClient<K, S> provideFsCrdtClient(Eventloop eventloop, LocalFsClient localFsClient, CrdtDescriptor<K, S> descriptor, Config config) {
			return FsCrdtClient.create(eventloop, localFsClient, descriptor.getCombiner(), descriptor.getSerializer())
					.initialize(Initializers.ofFsCrdtClient(config.getChild("crdt.files")));
		}
	}
}
