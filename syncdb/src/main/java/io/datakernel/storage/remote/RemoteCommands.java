package io.datakernel.storage.remote;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

class RemoteCommands {
	static Gson commandGSON = new GsonBuilder()
			.registerTypeAdapter(RemoteCommand.class, GsonSubclassesAdapter.create()
					.withSubclassField("commandType")
					.withSubclass("GetSortedStream", GetSortedOutput.class))
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	static abstract class RemoteCommand {}

	public static final class GetSortedOutput extends RemoteCommand {
		private final String predicateString;

		GetSortedOutput(String predicateString) {
			this.predicateString = predicateString;
		}

		String getPredicateString() {
			return predicateString;
		}

		@Override
		public String toString() {
			return "GetSortedStream";
		}
	}

	public static final class GetSortedInput extends RemoteCommand {

		@Override
		public String toString() {
			return "GetSortedInput";
		}
	}
}
