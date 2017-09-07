package io.datakernel.storage.streams;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.AbstractStreamTransformer_1_1;
import io.datakernel.stream.StreamDataReceiver;

import java.util.function.Function;
import java.util.function.Predicate;

// refactor StreamFilter like this
public final class StreamKeyFilter<K, V> extends AbstractStreamTransformer_1_1<V, V> {
	private final InputConsumer inputConsumer;
	private final OutputProducer outputProducer;

	public StreamKeyFilter(Eventloop eventloop, Predicate<K> filter, Function<V, K> function) {
		super(eventloop);
		this.inputConsumer = new InputConsumer();
		this.outputProducer = new OutputProducer(filter, function);
	}

	private final class InputConsumer extends AbstractInputConsumer {

		@Override
		protected void onUpstreamEndOfStream() {
			outputProducer.sendEndOfStream();
		}

		@Override
		public StreamDataReceiver<V> getDataReceiver() {
			return outputProducer;
		}
	}

	private final class OutputProducer extends AbstractOutputProducer implements StreamDataReceiver<V> {
		private final Predicate<K> filter;
		private final Function<V, K> function;

		private OutputProducer(Predicate<K> filter, Function<V, K> function) {
			this.filter = filter;
			this.function = function;
		}

		@Override
		protected void onDownstreamSuspended() {
			inputConsumer.suspend();
		}

		@Override
		protected void onDownstreamResumed() {
			inputConsumer.resume();
		}

		@Override
		public void onData(V item) {
			if (filter.test(function.apply(item))) {
				send(item);
			}
		}
	}

	@Override
	protected AbstractInputConsumer getInputImpl() {
		return inputConsumer;
	}

	@Override
	protected AbstractOutputProducer getOutputImpl() {
		return outputProducer;
	}
}
