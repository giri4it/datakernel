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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;

import java.net.InetAddress;

import static io.datakernel.config.ConfigConverters.ofInetAddress;
import static io.datakernel.config.ConfigConverters.ofInteger;

public class ConfigModuleExample extends AbstractModule {
	private static final String PROPERTIES_FILE = "example.properties";

	@Provides
	String providePhrase(Config config) {
		return config.get("phrase");
	}

	@Provides
	Integer provideNumber(Config config) {
		return config.get(ofInteger(), "number");
	}

	@Provides
	InetAddress provideAddress(Config config) {
		return config.get(ofInetAddress(), "address");
	}

	public static void main(String[] args) {
		Injector injector = Guice.createInjector(
				new ConfigModuleExample(),
				ConfigModule.create(Config.ofProperties(PROPERTIES_FILE))
		);

		String phrase = injector.getInstance(String.class);
		Integer number = injector.getInstance(Integer.class);
		InetAddress address = injector.getInstance(InetAddress.class);

		System.out.println(phrase);
		System.out.println(number);
		System.out.println(address);
	}
}
