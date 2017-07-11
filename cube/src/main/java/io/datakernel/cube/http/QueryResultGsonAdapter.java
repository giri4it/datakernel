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

package io.datakernel.cube.http;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.cube.QueryResult;
import io.datakernel.cube.Record;
import io.datakernel.cube.RecordScheme;
import io.datakernel.cube.ReportType;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static io.datakernel.cube.ReportType.*;
import static io.datakernel.cube.http.Utils.*;

final class QueryResultGsonAdapter extends TypeAdapter<QueryResult> {
	private static final String MEASURES_FIELD = "measures";
	private static final String ATTRIBUTES_FIELD = "attributes";
	private static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	private static final String RECORDS_FIELD = "records";
	private static final String TOTALS_FIELD = "totals";
	private static final String COUNT_FIELD = "count";
	private static final String SORTED_BY_FIELD = "sortedBy";
	private static final String REPORT_TYPE = "reportType";
	private static final String METADATA_FIELD = "metadata";

	private final Map<String, QueryResultCodec> resultTypeCodecs = ImmutableMap.<String, QueryResultCodec>builder()
			.put(METADATA_REPORT, new MetadataFormatter())
			.put(DATA_REPORT, new DataFormatter())
			.put(DATA_WITH_TOTALS_REPORT, new DataWithTotalsFormatter())
			.put(RESOLVE_ATTRIBUTES_REPORT, new ResolveAttributesFormatter())
			.build();

	private final Map<String, TypeAdapter<?>> attributeAdapters;
	private final Map<String, TypeAdapter<?>> measureAdapters;

	private final Map<String, Class<?>> attributeTypes;
	private final Map<String, Class<?>> measureTypes;

	private final TypeAdapter<List<String>> stringListAdapter;

	public QueryResultGsonAdapter(Map<String, TypeAdapter<?>> attributeAdapters, Map<String, TypeAdapter<?>> measureAdapters, Map<String, Class<?>> attributeTypes, Map<String, Class<?>> measureTypes, TypeAdapter<List<String>> stringListAdapter) {
		this.attributeAdapters = attributeAdapters;
		this.measureAdapters = measureAdapters;
		this.attributeTypes = attributeTypes;
		this.measureTypes = measureTypes;
		this.stringListAdapter = stringListAdapter;
	}

	public static QueryResultGsonAdapter create(Gson gson, Map<String, Type> attributeTypes, Map<String, Type> measureTypes) {
		Map<String, TypeAdapter<?>> attributeAdapters = newLinkedHashMap();
		Map<String, TypeAdapter<?>> measureAdapters = newLinkedHashMap();
		Map<String, Class<?>> attributeRawTypes = newLinkedHashMap();
		Map<String, Class<?>> measureRawTypes = newLinkedHashMap();
		for (String attribute : attributeTypes.keySet()) {
			TypeToken<?> typeToken = TypeToken.get(attributeTypes.get(attribute));
			attributeAdapters.put(attribute, gson.getAdapter(typeToken));
			attributeRawTypes.put(attribute, typeToken.getRawType());
		}
		for (String measure : measureTypes.keySet()) {
			TypeToken<?> typeToken = TypeToken.get(measureTypes.get(measure));
			measureAdapters.put(measure, gson.getAdapter(typeToken));
			measureRawTypes.put(measure, typeToken.getRawType());
		}
		TypeAdapter<List<String>> stringListAdapter = gson.getAdapter(new TypeToken<List<String>>() {});
		return new QueryResultGsonAdapter(attributeAdapters, measureAdapters, attributeRawTypes, measureRawTypes,
				stringListAdapter);
	}

	@Override
	public QueryResult read(JsonReader reader) throws JsonParseException, IOException {
		reader.beginObject();

		checkArgument(REPORT_TYPE.equals(reader.nextName()));
		String resultType = reader.nextString();

		final QueryResultCodec formatter = resultTypeCodecs.get(resultType);
		checkNotNull(formatter);

		QueryResult result = formatter.read(reader);
		reader.endObject();

		return result;
	}

	private List<Record> readRecords(JsonReader reader, RecordScheme recordScheme) throws JsonParseException, IOException {
		List<Record> records = new ArrayList<>();

		TypeAdapter[] fieldTypeAdapters = getTypeAdapters(recordScheme);

		reader.beginArray();
		while (reader.hasNext()) {
			reader.beginArray();
			Record record = Record.create(recordScheme);
			for (int i = 0; i < fieldTypeAdapters.length; i++) {
				Object fieldValue = fieldTypeAdapters[i].read(reader);
				record.put(i, fieldValue);
			}
			records.add(record);
			reader.endArray();
		}
		reader.endArray();

		return records;
	}

	private Record readTotals(JsonReader reader, RecordScheme recordScheme) throws JsonParseException, IOException {
		reader.beginArray();
		Record totals = Record.create(recordScheme);
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			TypeAdapter<?> fieldTypeAdapter = measureAdapters.get(field);
			if (fieldTypeAdapter == null)
				continue;
			Object fieldValue = fieldTypeAdapter.read(reader);
			totals.put(i, fieldValue);
		}
		reader.endArray();
		return totals;
	}

	private Map<String, Object> readFilterAttributes(JsonReader reader) throws JsonParseException, IOException {
		reader.beginObject();
		Map<String, Object> result = newLinkedHashMap();
		while (reader.hasNext()) {
			String attribute = reader.nextName();
			Object value = attributeAdapters.get(attribute).read(reader);
			result.put(attribute, value);
		}
		reader.endObject();
		return result;
	}

	@Override
	public void write(JsonWriter writer, QueryResult result) throws IOException {
		writer.beginObject();

		final ReportType resultType = result.getResultType();
		String type = getResultTypeName(resultType);

		final QueryResultCodec formatter = resultTypeCodecs.get(type);

		formatter.write(writer, result);

		writer.endObject();
	}

	@SuppressWarnings("unchecked")
	private void writeRecords(JsonWriter writer, RecordScheme recordScheme, List<Record> records) throws IOException {
		writer.beginArray();

		TypeAdapter[] fieldTypeAdapters = getTypeAdapters(recordScheme);

		for (Record record : records) {
			writer.beginArray();
			for (int i = 0; i < recordScheme.getFields().size(); i++) {
				fieldTypeAdapters[i].write(writer, record.get(i));
			}
			writer.endArray();
		}
		writer.endArray();
	}

	@SuppressWarnings("unchecked")
	private void writeTotals(JsonWriter writer, RecordScheme recordScheme, Record totals) throws IOException {
		writer.beginArray();
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			TypeAdapter fieldTypeAdapter = measureAdapters.get(field);
			if (fieldTypeAdapter == null)
				continue;
			fieldTypeAdapter.write(writer, totals.get(i));
		}
		writer.endArray();
	}

	@SuppressWarnings("unchecked")
	private void writeFilterAttributes(JsonWriter writer, Map<String, Object> filterAttributes) throws IOException {
		writer.beginObject();
		for (String attribute : filterAttributes.keySet()) {
			Object value = filterAttributes.get(attribute);
			writer.name(attribute);
			TypeAdapter typeAdapter = attributeAdapters.get(attribute);
			typeAdapter.write(writer, value);
		}
		writer.endObject();
	}

	public RecordScheme recordScheme(List<String> attributes, List<String> measures) {
		RecordScheme recordScheme = RecordScheme.create();
		for (String attribute : attributes) {
			recordScheme = recordScheme.withField(attribute, attributeTypes.get(attribute));
		}
		for (String measure : measures) {
			recordScheme = recordScheme.withField(measure, measureTypes.get(measure));
		}
		return recordScheme;
	}

	private TypeAdapter[] getTypeAdapters(RecordScheme recordScheme) {
		TypeAdapter[] fieldTypeAdapters = new TypeAdapter[recordScheme.getFields().size()];
		for (int i = 0; i < recordScheme.getFields().size(); i++) {
			String field = recordScheme.getField(i);
			fieldTypeAdapters[i] = firstNonNull(attributeAdapters.get(field), measureAdapters.get(field));
		}
		return fieldTypeAdapters;
	}

	// region helper classes
	private interface QueryResultCodec {

		void write(JsonWriter writer, QueryResult result) throws IOException;

		QueryResult read(JsonReader reader) throws IOException;

	}

	private final class MetadataFormatter implements QueryResultCodec {

		@Override
		public void write(JsonWriter writer, QueryResult result) throws IOException {
			writer.name(METADATA_FIELD);
			writer.beginObject();

			writer.name(ATTRIBUTES_FIELD);
			stringListAdapter.write(writer, result.getAttributes());

			writer.name(MEASURES_FIELD);
			stringListAdapter.write(writer, result.getMeasures());

			writer.endObject();
		}

		@Override
		public QueryResult read(JsonReader reader) throws IOException {
			checkArgument(METADATA_FIELD.equals(reader.nextName()));
			reader.beginObject();

			checkArgument(ATTRIBUTES_FIELD.equals(reader.nextName()));
			final List<String> attributes = stringListAdapter.read(reader);

			checkArgument(MEASURES_FIELD.equals(reader.nextName()));
			final List<String> measures = stringListAdapter.read(reader);

			reader.endObject();

			final RecordScheme recordScheme = RecordScheme.create();
			return QueryResult.create(recordScheme, Collections.<Record>emptyList(), Record.create(recordScheme), 0,
					attributes, measures, Collections.<String>emptyList(), Collections.<String, Object>emptyMap(),
					METADATA);
		}

	}

	private final class DataFormatter implements QueryResultCodec {
		private final QueryResultCodec metadataFormatter = new MetadataFormatter();

		@Override
		public void write(JsonWriter writer, QueryResult result) throws IOException {
			metadataFormatter.write(writer, result);

			writer.name(SORTED_BY_FIELD);
			stringListAdapter.write(writer, result.getSortedBy());

			writer.name(RECORDS_FIELD);
			writeRecords(writer, result.getRecordScheme(), result.getRecords());

			writer.name(COUNT_FIELD);
			writer.value(result.getTotalCount());

			writer.name(FILTER_ATTRIBUTES_FIELD);
			writeFilterAttributes(writer, result.getFilterAttributes());
		}

		@Override
		public QueryResult read(JsonReader reader) throws IOException {
			QueryResult result = metadataFormatter.read(reader);

			checkArgument(SORTED_BY_FIELD.equals(reader.nextName()));
			List<String> sortedBy = stringListAdapter.read(reader);

			RecordScheme recordScheme = recordScheme(result.getAttributes(), result.getMeasures());

			checkArgument(RECORDS_FIELD.equals(reader.nextName()));
			List<Record> records = readRecords(reader, recordScheme);

			checkArgument(COUNT_FIELD.equals(reader.nextName()));
			int count = reader.nextInt();

			// TODO: 30.06.17 remove after implementing separata resolver servlet
			checkArgument(FILTER_ATTRIBUTES_FIELD.equals(reader.nextName()));
			Map<String, Object> filterAttributes = readFilterAttributes(reader);

			return QueryResult.create(recordScheme, records, result.getTotals(), count, result.getAttributes(),
					result.getMeasures(), sortedBy, filterAttributes, DATA);
		}

	}

	private final class DataWithTotalsFormatter implements QueryResultCodec {
		private final QueryResultCodec dataFormatter = new DataFormatter();

		@Override
		public void write(JsonWriter writer, QueryResult result) throws IOException {
			dataFormatter.write(writer, result);

			writer.name(TOTALS_FIELD);
			writeTotals(writer, result.getRecordScheme(), result.getTotals());
		}

		@Override
		public QueryResult read(JsonReader reader) throws IOException {
			QueryResult result = dataFormatter.read(reader);

			RecordScheme recordScheme = recordScheme(result.getAttributes(), result.getMeasures());
			checkArgument(TOTALS_FIELD.equals(reader.nextName()));
			Record totals = readTotals(reader, recordScheme);

			return QueryResult.create(result.getRecordScheme(), result.getRecords(), totals, result.getTotalCount(),
					result.getAttributes(), result.getMeasures(), result.getSortedBy(), result.getFilterAttributes(),
					DATA_WITH_TOTALS);
		}

	}

	private final class ResolveAttributesFormatter implements QueryResultCodec {
		private final QueryResultCodec metadataFormatter = new MetadataFormatter();

		@Override
		public void write(JsonWriter writer, QueryResult result) throws IOException {
			metadataFormatter.write(writer, result);

			writer.name(FILTER_ATTRIBUTES_FIELD);
			writeFilterAttributes(writer, result.getFilterAttributes());
		}

		@Override
		public QueryResult read(JsonReader reader) throws IOException {
			QueryResult result = metadataFormatter.read(reader);

			checkArgument(FILTER_ATTRIBUTES_FIELD.equals(reader.nextName()));
			Map<String, Object> filterAttributes = readFilterAttributes(reader);

			return QueryResult.create(result.getRecordScheme(), result.getRecords(), result.getTotals(), result.getTotalCount(),
					result.getAttributes(), result.getMeasures(), result.getSortedBy(), filterAttributes, RESOLVE_ATTRIBUTES);
		}

	}

	private static String getResultTypeName(ReportType resultType) {
		String type;
		switch (resultType) {
			case METADATA:
				type = METADATA_REPORT;
				break;
			case DATA:
				type = DATA_REPORT;
				break;
			case DATA_WITH_TOTALS:
				type = DATA_WITH_TOTALS_REPORT;
				break;
			case RESOLVE_ATTRIBUTES:
				type = RESOLVE_ATTRIBUTES_REPORT;
				break;
			default:
				throw new IllegalArgumentException("Unexpected query result type: " + resultType);
		}
		return type;
	}
}
