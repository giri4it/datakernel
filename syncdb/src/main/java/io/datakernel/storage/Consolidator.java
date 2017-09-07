package io.datakernel.storage;

import java.util.concurrent.CompletionStage;

public interface Consolidator {

	CompletionStage<Void> consolidate();
}
