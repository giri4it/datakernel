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

package io.datakernel.stream;

import io.datakernel.async.Stage;

import java.util.Set;

public abstract class ForwardingStreamProducer<T> implements StreamProducer<T> {
	protected final StreamProducer<T> producer;

	private StreamLogger streamLogger;

	public ForwardingStreamProducer(StreamProducer<T> producer, String tag) {
		this.producer = producer;
		streamLogger = producer.getStreamLogger().createChild(this, tag + "->" + producer.getStreamLogger().getTag());
	}

	public ForwardingStreamProducer(StreamProducer<T> producer) {
		this(producer, "forwarded");
	}

	@Override
	public void setConsumer(StreamConsumer<T> consumer) {
		producer.setConsumer(consumer);
	}

	@Override
	public void produce(StreamDataReceiver<T> dataReceiver) {
		producer.produce(dataReceiver);
	}

	@Override
	public void suspend() {
		producer.suspend();
	}

	@Override
	public Stage<Void> getEndOfStream() {
		return producer.getEndOfStream();
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return producer.getCapabilities();
	}

	@Override
	public StreamLogger getStreamLogger() {
		return streamLogger;
	}

	@Override
	public void setStreamLogger(StreamLogger streamLogger) {
		this.streamLogger = streamLogger;
	}

	@Override
	public String toString() {
		return streamLogger.getTag();
	}
}
