package io.datakernel.stream.processor;

import io.datakernel.stream.HasInput;
import io.datakernel.stream.HasOutputs;
import io.datakernel.stream.StreamVisitor;
import io.datakernel.stream.StreamVisitor.StreamVisitable;

public interface StreamTransformerOneToMany<I> extends HasInput<I>, HasOutputs, StreamVisitable {

	@Override
	default void accept(StreamVisitor visitor) {
		visitor.visitTransformer(this);
	}
}
