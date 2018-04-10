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
	protected final StreamProducer<T> peer;

	public ForwardingStreamProducer(StreamProducer<T> peer) {
		this.peer = peer;
	}

	@Override
	public void setConsumer(StreamConsumer<T> consumer) {
		peer.setConsumer(consumer);
	}

	@Override
	public void produce(StreamDataReceiver<T> dataReceiver) {
		peer.produce(dataReceiver);
	}

	@Override
	public void suspend() {
		peer.suspend();
	}

	@Override
	public Stage<Void> getEndOfStream() {
		return peer.getEndOfStream();
	}

	@Override
	public Set<StreamCapability> getCapabilities() {
		return peer.getCapabilities();
	}

	@Override
	public void accept(StreamVisitor visitor) {
		visitor.visitForwarding(peer, this);
		StreamProducer.super.accept(visitor);
	}
}