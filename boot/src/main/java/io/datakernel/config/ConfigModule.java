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

package io.datakernel.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import io.datakernel.service.BlockingService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.RequiredDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;

public final class ConfigModule extends AbstractModule implements Initializable<ConfigModule> {
	private static final Logger logger = LoggerFactory.getLogger(ConfigModule.class);

	private Supplier<Config> configSupplier;
	private Path effectiveConfigPath;
	private Consumer<String> effectiveConfigConsumer;
	private volatile boolean started;

	private class ProtectedConfig implements Config {
		private final Config config;
		private final Map<String, Config> children;

		private ProtectedConfig(Config config) {
			this.config = config;
			this.children = new LinkedHashMap<>();
			config.getChildren().forEach((key, value) ->
					this.children.put(key, new ProtectedConfig(value)));
		}

		@Override
		public String getValue(String defaultValue) {
			checkState(!started, "Config must be used during application start-up time only");
			return config.getValue(defaultValue);
		}

		@Override
		public String getValue() throws NoSuchElementException {
			checkState(!started, "Config must be used during application start-up time only");
			return config.getValue();
		}

		@Override
		public Map<String, Config> getChildren() {
			return children;
		}

		@Override
		public Config provideNoKeyChild(String key) {
			checkArgument(!children.keySet().contains(key));
			return new ProtectedConfig(config.provideNoKeyChild(key));
		}
	}

	private interface ConfigSaveService extends BlockingService {
	}

	private ConfigModule(Supplier<Config> configSupplier) {
		this.configSupplier = configSupplier;
	}

	public static ConfigModule create(Supplier<Config> configSupplier) {
		return new ConfigModule(configSupplier);
	}

	public static ConfigModule create(Config config) {
		return new ConfigModule(() -> config);
	}

	public ConfigModule saveEffectiveConfigTo(String file) {
		return saveEffectiveConfigTo(Paths.get(file));
	}

	public ConfigModule saveEffectiveConfigTo(Path file) {
		this.effectiveConfigPath = file;
		return this;
	}

	public ConfigModule writeEffectiveConfigTo(Writer writer) {
		this.effectiveConfigConsumer = effectiveConfig -> {
			try {
				writer.write(effectiveConfig);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		};
		return this;
	}

	public ConfigModule writeEffectiveConfigTo(PrintStream writer) {
		this.effectiveConfigConsumer = writer::print;
		return this;
	}

	public ConfigModule printEffectiveConfig() {
		this.effectiveConfigConsumer = effectiveConfig -> {
			System.out.println("# Effective config:\n");
			System.out.println(effectiveConfig);
		};
		return this;
	}

	@Override
	protected void configure() {
		bind(new TypeLiteral<RequiredDependency<ServiceGraph>>() {}).asEagerSingleton();
		bind(new TypeLiteral<RequiredDependency<ConfigSaveService>>() {}).asEagerSingleton();
		bind(Config.class).to(EffectiveConfig.class);
	}

	@Provides
	@Singleton
	EffectiveConfig provideConfig() {
		Config config = new ProtectedConfig(ConfigWithFullPath.wrap(configSupplier.get()));
		return EffectiveConfig.wrap(config);
	}

	@Provides
	@Singleton
	ConfigSaveService configSaveService(EffectiveConfig config,
	                                    OptionalDependency<Initializer<ConfigModule>> maybeInitializer) {
		maybeInitializer.ifPresent(initializer -> initializer.accept(this));
		return new ConfigSaveService() {
			@Override
			public void start() {
				started = true;
				if (effectiveConfigPath != null) {
					logger.info("Saving effective config to {}", effectiveConfigPath);
					config.saveEffectiveConfigTo(effectiveConfigPath);
				}
				if (effectiveConfigConsumer != null) {
					effectiveConfigConsumer.accept(config.renderEffectiveConfig());
				}
			}

			@Override
			public void stop() {
			}
		};
	}

}