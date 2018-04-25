package io.datakernel.stream;

public interface HasLogging {

	default void setLogger(StreamLogger streamLogger) {
		if (this instanceof HasInput) {
			StreamConsumer input = ((HasInput) this).getInput();
			input.setStreamLogger(streamLogger.createChild(input, "input->" + streamLogger.getTag()));
		}
		if (this instanceof HasInputs) {
			int i = 0;
			for (StreamConsumer<?> input : ((HasInputs) this).getInputs()) {
				input.setStreamLogger(streamLogger.createChild(input, "input#" + ++i + "->" + streamLogger.getTag()));
			}
		}
		if (this instanceof HasOutput) {
			StreamProducer output = ((HasOutput) this).getOutput();
			output.setStreamLogger(streamLogger.createChild(output, "output->" + streamLogger.getTag()));
		}
		if (this instanceof HasOutputs) {
			int i = 0;
			for (StreamProducer<?> output : ((HasOutputs) this).getOutputs()) {
				output.setStreamLogger(streamLogger.createChild(output, "output#" + ++i + "->" + streamLogger.getTag()));
			}
		}
	}
}
