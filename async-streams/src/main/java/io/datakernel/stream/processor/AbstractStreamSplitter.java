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

package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_N;
import io.datakernel.stream.StreamDataReceiver;

public abstract class AbstractStreamSplitter<I> extends AbstractStreamTransformer_1_N<I>{

	protected abstract class InputConsumer extends AbstractInputConsumer implements StreamDataReceiver<I> {
		@Override
		protected void onUpstreamEndOfStream() {
			for (AbstractOutputProducer<?> downstreamProducer : outputProducers) {
				downstreamProducer.sendEndOfStream();
			}
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return this;
		}

		public abstract void onData(I item);
	}

	protected class OutputProducer<O> extends AbstractOutputProducer<O> {
		public OutputProducer() {
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			if (allOutputsResumed()) {
				inputConsumer.resume();
			}
		}
	}

	/**
	 * Creates a new instance of this object
	 *
	 * @param eventloop event loop in which this consumer will run
	 */
	public AbstractStreamSplitter(Eventloop eventloop) {
		super(eventloop);
	}
}
