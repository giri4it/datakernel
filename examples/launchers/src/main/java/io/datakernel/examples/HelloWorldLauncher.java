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
import io.datakernel.launcher.Launcher;

import java.util.Collection;
import java.util.Collections;

public class HelloWorldLauncher {
	public static void main(String[] args) throws Exception {
		Launcher launcher = new Launcher() {
			@Override
			protected Collection<Module> getModules() {
				return Collections.emptyList();
			}

			@Override
			protected void run() {
				System.out.println("Hello World!");
			}
		};
		launcher.launch(true, args);
	}
}
