package io.datakernel.stream.processor;

import io.datakernel.stream.HasInputs;
import io.datakernel.stream.HasOutput;
import io.datakernel.stream.StreamVisitor;
import io.datakernel.stream.StreamVisitor.StreamVisitable;

public interface StreamTransformerManyToOne<O> extends HasInputs, HasOutput<O>, StreamVisitable {

	@Override
	default void accept(StreamVisitor visitor) {
		visitor.visitTransformer(this);
	}
}
