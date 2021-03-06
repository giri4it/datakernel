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

import com.google.inject.Module;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.util.Collection;

import static java.util.Arrays.asList;

public class UIKernelWebAppLauncher extends Launcher {

	@Override
	protected Collection<Module> getModules() {
		return asList(ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofProperties("configs.properties")),
				new UIKernelWebAppModule());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		UIKernelWebAppLauncher launcher = new UIKernelWebAppLauncher();
		launcher.launch(true, args);
	}
}
