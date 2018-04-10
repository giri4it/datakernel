package io.datakernel.stream;

import io.datakernel.annotation.Nullable;
import io.datakernel.stream.processor.StreamTransformer;
import io.datakernel.stream.processor.StreamTransformerManyToOne;
import io.datakernel.stream.processor.StreamTransformerOneToMany;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;

public final class StreamVisitor {

	private Set<StreamVisitable> visited = new HashSet<>();

	private Consumer<StreamProducer<?>> producerCallback = $ -> {};
	private Consumer<StreamConsumer<?>> consumerCallback = $ -> {};

	private BiConsumer<StreamProducer<?>, StreamConsumer<?>> producerConsumerConnectionCallback = ($1, $2) -> {};
	private BiConsumer<StreamConsumer<?>, StreamProducer<?>> consumerProducerConnectionCallback = ($1, $2) -> {};

	private BiConsumer<StreamProducer<?>, StreamProducer<?>> forwardingProducerCallback = ($1, $2) -> {};
	private BiConsumer<StreamConsumer<?>, StreamConsumer<?>> forwardingConsumerCallback = ($1, $2) -> {};

	private Consumer<StreamTransformer<?, ?>> oneToOneTransformerCallback = $ -> {};
	private Consumer<StreamTransformerOneToMany<?>> oneToManyTransformerCallback = $ -> {};
	private Consumer<StreamTransformerManyToOne<?>> manyToOneTransformerCallback = $ -> {};

	private GenericTransformerCallback transformerCallback = ($1, $2, $3) -> {};

	// region creators
	private StreamVisitor() {
	}

	public static StreamVisitor create() {
		return new StreamVisitor();
	}
	// endregion

	public StreamVisitor onProducer(Consumer<StreamProducer<?>> producerCallback) {
		this.producerCallback = producerCallback;
		return this;
	}

	public StreamVisitor onConsumer(Consumer<StreamConsumer<?>> consumerCallback) {
		this.consumerCallback = consumerCallback;
		return this;
	}

	public StreamVisitor onProducerConsumerConnection(BiConsumer<StreamProducer<?>, StreamConsumer<?>> producerConsumerConnectionCallback) {
		this.producerConsumerConnectionCallback = producerConsumerConnectionCallback;
		return this;
	}

	public StreamVisitor onConsumerProducerConnection(BiConsumer<StreamConsumer<?>, StreamProducer<?>> consumerProducerConnectionCallback) {
		this.consumerProducerConnectionCallback = consumerProducerConnectionCallback;
		return this;
	}

	public StreamVisitor onProducerForwarding(BiConsumer<StreamProducer<?>, StreamProducer<?>> forwardingProducerCallback) {
		this.forwardingProducerCallback = forwardingProducerCallback;
		return this;
	}

	public StreamVisitor onConsumerForwarding(BiConsumer<StreamConsumer<?>, StreamConsumer<?>> forwardingConsumerCallback) {
		this.forwardingConsumerCallback = forwardingConsumerCallback;
		return this;
	}

	public StreamVisitor onTransformer(GenericTransformerCallback transformerCallback) {
		this.transformerCallback = transformerCallback;
		return this;
	}

	public StreamVisitor onOneToOneTransformer(Consumer<StreamTransformer<?, ?>> transformerCallback) {
		oneToOneTransformerCallback = transformerCallback;
		return this;
	}

	public StreamVisitor onOneToManyTransformer(Consumer<StreamTransformerOneToMany<?>> oneToManyTransformerCallback) {
		this.oneToManyTransformerCallback = oneToManyTransformerCallback;
		return this;
	}

	public StreamVisitor onManyToOneTransformer(Consumer<StreamTransformerManyToOne<?>> manyToOneTransformerCallback) {
		this.manyToOneTransformerCallback = manyToOneTransformerCallback;
		return this;
	}

	public void visitProducer(StreamProducer<?> producer) {
		if (visited.add(producer)) {
			producerCallback.accept(producer);
		}
	}

	public void visitConsumer(StreamConsumer<?> consumer) {
		if (visited.add(consumer)) {
			consumerCallback.accept(consumer);
		}
	}

	public void visitProducerConsumerConnection(StreamProducer<?> producer, @Nullable StreamConsumer<?> consumer) {
		if (consumer != null) {
			StreamVisitablePair pair = new StreamVisitablePair(producer, consumer);
			if (visited.add(pair)) {
				pair.accept(this);
				producerConsumerConnectionCallback.accept(producer, consumer);
			}
		}
	}

	public void visitConsumerProducerConnection(StreamConsumer<?> consumer, @Nullable StreamProducer<?> producer) {
		if (producer != null) {
			StreamVisitablePair pair = new StreamVisitablePair(consumer, producer);
			if (visited.add(pair)) {
				pair.accept(this);
				consumerProducerConnectionCallback.accept(consumer, producer);
			}
		}
	}

	public void visitForwarding(StreamProducer<?> actual, StreamProducer<?> forwarder) {
		if (visited.add(new StreamVisitablePair(actual, forwarder))) {
			actual.accept(this);
			forwardingProducerCallback.accept(actual, forwarder);
		}
	}

	public void visitForwarding(StreamConsumer<?> actual, StreamConsumer<?> forwarder) {
		if (visited.add(new StreamVisitablePair(actual, forwarder))) {
			actual.accept(this);
			forwardingConsumerCallback.accept(actual, forwarder);
		}
	}

	public void visitTransformer(StreamTransformer<?, ?> transformer) {
		if (visited.add(transformer)) {
			StreamConsumer<?> input = transformer.getInput();
			StreamProducer<?> output = transformer.getOutput();
			input.accept(this);
			output.accept(this);
			oneToOneTransformerCallback.accept(transformer);
			transformerCallback.accept(transformer, singletonList(input), singletonList(output));
		}
	}

	public void visitTransformer(StreamTransformerOneToMany<?> transformer) {
		if (visited.add(transformer)) {
			StreamConsumer<?> input = transformer.getInput();
			List<? extends StreamProducer<?>> outputs = transformer.getOutputs();
			input.accept(this);
			for (StreamProducer<?> output : outputs) {
				output.accept(this);
			}
			oneToManyTransformerCallback.accept(transformer);
			transformerCallback.accept(transformer, singletonList(input), outputs);
		}
	}

	public void visitTransformer(StreamTransformerManyToOne<?> transformer) {
		if (visited.add(transformer)) {
			List<? extends StreamConsumer<?>> inputs = transformer.getInputs();
			StreamProducer<?> output = transformer.getOutput();
			for (StreamConsumer<?> input : inputs) {
				input.accept(this);
			}
			output.accept(this);
			manyToOneTransformerCallback.accept(transformer);
			transformerCallback.accept(transformer, inputs, singletonList(output));
		}
	}

	@FunctionalInterface
	public interface GenericTransformerCallback {

		void accept(Object transformer, List<? extends StreamConsumer<?>> inputs, List<? extends StreamProducer<?>> outputs);
	}

	@FunctionalInterface
	public interface StreamVisitable {

		void accept(StreamVisitor visitor);
	}

	private static final class StreamVisitablePair implements StreamVisitable {

		private StreamVisitable first;
		private StreamVisitable second;

		public StreamVisitablePair(StreamVisitable first, StreamVisitable second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public void accept(StreamVisitor visitor) {
			if (first != null) {
				first.accept(visitor);
			}
			if (second != null) {
				second.accept(visitor);
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			StreamVisitablePair that = (StreamVisitablePair) o;

			return first.equals(that.first) && second.equals(that.second);
		}

		@Override
		public int hashCode() {
			return 31 * first.hashCode() + second.hashCode();
		}
	}
}
