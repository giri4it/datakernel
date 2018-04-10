package io.datakernel.stream;

import io.datakernel.stream.StreamVisitor.StreamVisitable;

public class DataStreams {

	public static <T> void bind(StreamProducer<T> producer, StreamConsumer<T> consumer) {
		producer.setConsumer(consumer);
		consumer.setProducer(producer);
	}

	public static <T> StreamCompletion stream(StreamProducer<T> producer,
											  StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X> StreamProducerResult<X> stream(StreamProducerWithResult<T, X> producer,
														StreamConsumer<T> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, Y> StreamConsumerResult<Y> stream(StreamProducer<T> producer,
														StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

	public static <T, X, Y> StreamResult<X, Y> stream(StreamProducerWithResult<T, X> producer,
													  StreamConsumerWithResult<T, Y> consumer) {
		return producer.streamTo(consumer);
	}

	private static String getClassName(Class<?> cls) {
		Package pkg = cls.getPackage();
		if (pkg != null) {
			return cls.getName().substring(pkg.getName().length() + 1);
		}
		return cls.getName();
	}

	public static String render(StreamVisitable visitable) {
		StringBuilder sb = new StringBuilder("digraph Stream {\n");

		visitable.accept(StreamVisitor.create()
				.onProducerConsumerConnection((p, c) -> sb.append("    \"").append(c).append("\" -> \"").append(p).append("\"[color=orange]\n"))
				.onConsumerProducerConnection((c, p) -> sb.append("    \"").append(p).append("\" -> \"").append(c).append("\"[color=blue]\n"))
				.onProducerForwarding((a, f) -> sb.append("    \"").append(a).append("\" -> \"").append(f).append("\"[arrowhead=tee]\n"))
				.onConsumerForwarding((a, f) -> sb.append("    \"").append(a).append("\" -> \"").append(f).append("\"[arrowhead=tee]\n"))
				.onTransformer((t, ins, outs) -> {
					String name = getClassName(t.getClass());
					sb.append("    subgraph cluster_")
							.append(name)
							.append(" {\n        graph[color=red, label=\"")
							.append(name)
							.append("\"]\n");
					for (StreamConsumer<?> in : ins) {
						for (StreamProducer<?> out : outs) {
							sb.append("        \"").append(in).append("\" -> \"").append(out).append("\"\n");
						}
					}
					sb.append("    }\n");
				}));

		return sb.append('}').toString();
	}
}
