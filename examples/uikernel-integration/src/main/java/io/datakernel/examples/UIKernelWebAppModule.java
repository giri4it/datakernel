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

package io.datakernel.examples;

import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.MiddlewareServlet;
import io.datakernel.http.StaticServlet;
import io.datakernel.loader.StaticLoader;
import io.datakernel.loader.StaticLoaders;
import io.datakernel.uikernel.UiKernelServlets;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.ConfigConverters.ofInteger;
import static io.datakernel.config.ConfigConverters.ofString;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class UIKernelWebAppModule extends AbstractModule {
	private static final int DEFAULT_PORT = 8080;
	private static final String DEFAULT_PATH_TO_RESOURCES = "src/main/resources/static/";

	@Override
	protected void configure() {
		bind(ExecutorService.class).toInstance(newCachedThreadPool());
		bind(PersonGridModel.class).in(Singleton.class);
		bind(Gson.class).in(Singleton.class);
	}

	@Provides
	@Singleton
	AsyncHttpServer server(Eventloop eventloop, ExecutorService executor, Gson gson, PersonGridModel model, Config config) {
		Path resources = Paths.get(config.get(ofString(), "resources", DEFAULT_PATH_TO_RESOURCES));
		StaticLoader resourceLoader = StaticLoaders.ofPath(executor, resources);
		int port = config.get(ofInteger(), "port", DEFAULT_PORT);

		// middleware used to map requests to appropriate asyncServlets

		StaticServlet staticServlet = StaticServlet.create(eventloop, resourceLoader);
		AsyncServlet usersApiServlet = UiKernelServlets.apiServlet(model, gson);

		MiddlewareServlet dispatcher = MiddlewareServlet.create()
				.withFallback(staticServlet)                 // serves request if no other servlet matches
				.with("/api/users", usersApiServlet);        // our rest crud servlet that would serve the grid

		// configuring server
		return AsyncHttpServer.create(eventloop, dispatcher).withListenPort(port);
	}

	@Provides
	@Singleton
	Eventloop eventloop() {
		return Eventloop.create();
	}
}
