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

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.cube.QueryResult;
import io.datakernel.cube.Record;
import io.datakernel.cube.RecordScheme;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

final class QueryResultGsonAdapter extends TypeAdapter<QueryResult> {
	private static final String MEASURES_FIELD = "measures";
	private static final String ATTRIBUTES_FIELD = "attributes";
	private static final String FILTER_ATTRIBUTES_FIELD = "filterAttributes";
	private static final String RECORDS_FIELD = "records";
	private static final String TOTALS_FIELD = "totals";
	private static final String COUNT_FIELD = "count";
	private static final String SORTED_BY_FIELD = "sortedBy";
	private static final String INCLUDED_IN_RESULT_FIELD = "includedInResult";
	private static final String METADATA_REPORT = "metadata";
	private static final String TOTALS_REPORT = "totals";
	private static final String DIMENSIONS_REPORT = "dimensions";
	private static final String RESOLVE_ATTRIBUTES_REPORT = "resolveAttributes";
	private static final String MEASURES_REPORT = "measures";

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

		checkArgument(INCLUDED_IN_RESULT_FIELD.equals(reader.nextName()));
		List<String> reportType = stringListAdapter.read(reader);

		final BitSet includedAspects = new BitSet(5);

		if (reportType.contains(METADATA_REPORT))
			includedAspects.set(0);
		if (reportType.contains(TOTALS_FIELD))
			includedAspects.set(1);
		if (reportType.contains(DIMENSIONS_REPORT))
			includedAspects.set(2);
		if (reportType.contains(MEASURES_REPORT))
			includedAspects.set(3);
		if (reportType.contains(RESOLVE_ATTRIBUTES_REPORT))
			includedAspects.set(4);

		List<String> attributes = emptyList();
		List<String> measures = emptyList();
		RecordScheme recordScheme = RecordScheme.create();
		List<Record> records = emptyList();
		List<String> sortedBy = emptyList();
		Record totals = Record.create(recordScheme);
		Map<String, Object> filterAttributes = emptyMap();
		int count = 0;

		if (includedAspects.isEmpty()) {
			checkArgument(ATTRIBUTES_FIELD.equals(reader.nextName()));
			attributes = stringListAdapter.read(reader);

			checkArgument(MEASURES_FIELD.equals(reader.nextName()));
			measures = stringListAdapter.read(reader);

			checkArgument(SORTED_BY_FIELD.equals(reader.nextName()));
			sortedBy = stringListAdapter.read(reader);

			recordScheme = recordScheme(attributes, measures);

			checkArgument(RECORDS_FIELD.equals(reader.nextName()));
			records = readRecords(reader, recordScheme);

			checkArgument(TOTALS_FIELD.equals(reader.nextName()));
			totals = readTotals(reader, recordScheme);

			checkArgument(COUNT_FIELD.equals(reader.nextName()));
			count = reader.nextInt();

			checkArgument(FILTER_ATTRIBUTES_FIELD.equals(reader.nextName()));
			filterAttributes = readFilterAttributes(reader);

		} else {
			if (reportType.contains(METADATA_REPORT)) {
				checkArgument(METADATA_REPORT.equals(reader.nextName()));
				reader.beginObject();

				checkArgument(ATTRIBUTES_FIELD.equals(reader.nextName()));
				attributes = stringListAdapter.read(reader);

				checkArgument(MEASURES_FIELD.equals(reader.nextName()));
				measures = stringListAdapter.read(reader);

				reader.endObject();
			}
			if (reportType.contains(TOTALS_FIELD)) {
				checkArgument(TOTALS_REPORT.equals(reader.nextName()));
				reader.beginObject();

				checkArgument(MEASURES_FIELD.equals(reader.nextName()));
				measures = stringListAdapter.read(reader);

				recordScheme = recordScheme(Collections.<String>emptyList(), measures);
				checkArgument(TOTALS_FIELD.equals(reader.nextName()));
				totals = readTotals(reader, recordScheme);

				reader.endObject();
			}
			if (reportType.contains(FILTER_ATTRIBUTES_FIELD)) {
				checkArgument(RESOLVE_ATTRIBUTES_REPORT.equals(reader.nextName()));
				reader.beginObject();

				checkArgument(FILTER_ATTRIBUTES_FIELD.equals(reader.nextName()));
				filterAttributes = readFilterAttributes(reader);

				reader.endObject();
			}
		}
		reader.endObject();
		return QueryResult.create(recordScheme, records, totals, count, attributes, measures, sortedBy,
				filterAttributes, includedAspects);
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

	/**
	 * Table of bit number to aspect correspondence:
	 * <table>
	 * <tr align="center">
	 * <td> Bit number </td> <td> Aspect included in result</td>
	 * </tr>
	 * <tr>
	 * <td align="right"> 0 </td> <td  align="left"> metadata </td>
	 * </tr>
	 * <tr>
	 * <td align="right"> 1 </td> <td  align="left"> totals </td>
	 * </tr>
	 * <tr>
	 * <td align="right"> 2 </td> <td  align="left"> dimensions </td>
	 * </tr>
	 * <tr>
	 * <td align="right"> 4 </td> <td  align="left"> measures </td>
	 * </tr>
	 * <tr>
	 * <td align="right"> 3 </td> <td  align="left"> resolve attributes </td>
	 * </tr>
	 * </table>
	 */
	@Override
	public void write(JsonWriter writer, QueryResult result) throws IOException {
		writer.beginObject();

		writer.name(INCLUDED_IN_RESULT_FIELD);
		List<String> reportTypes = new ArrayList<>();
		BitSet reportType = result.getReportType();
		if (reportType.get(0))
			reportTypes.add(METADATA_REPORT);
		if (reportType.get(1))
			reportTypes.add(TOTALS_REPORT);
		if (reportType.get(2))
			reportTypes.add(DIMENSIONS_REPORT);
		if (reportType.get(3))
			reportTypes.add(MEASURES_REPORT);
		if (reportType.get(4))
			reportTypes.add(RESOLVE_ATTRIBUTES_REPORT);
		stringListAdapter.write(writer, reportTypes);

		if (reportType.cardinality() == 0) {
			writer.name(ATTRIBUTES_FIELD);
			stringListAdapter.write(writer, result.getAttributes());

			writer.name(MEASURES_FIELD);
			stringListAdapter.write(writer, result.getMeasures());

			writer.name(SORTED_BY_FIELD);
			stringListAdapter.write(writer, result.getSortedBy());

			writer.name(RECORDS_FIELD);
			writeRecords(writer, result.getRecordScheme(), result.getRecords());

			writer.name(TOTALS_FIELD);
			writeTotals(writer, result.getRecordScheme(), result.getTotals());

			writer.name(COUNT_FIELD);
			writer.value(result.getTotalCount());

			writer.name(FILTER_ATTRIBUTES_FIELD);
			writeFilterAttributes(writer, result.getFilterAttributes());
		} else {
			if (reportType.get(0)) {
				writer.name(METADATA_REPORT);
				writer.beginObject();

				writer.name(ATTRIBUTES_FIELD);
				stringListAdapter.write(writer, result.getAttributes());

				writer.name(MEASURES_FIELD);
				stringListAdapter.write(writer, result.getMeasures());

				writer.endObject();
			}
			if (reportType.get(2)) {
				writer.name(DIMENSIONS_REPORT);
				writer.beginObject();

				writer.name(ATTRIBUTES_FIELD);
				stringListAdapter.write(writer, result.getAttributes());

				// TODO: 22.06.17 create record scheme without measures
				writer.name(RECORDS_FIELD);
				writeRecords(writer, result.getRecordScheme(), result.getRecords());

				writer.endObject();
			}
			if (reportType.get(3)) {
				writer.name(DIMENSIONS_REPORT);
				writer.beginObject();

				writer.name(ATTRIBUTES_FIELD);
				stringListAdapter.write(writer, result.getAttributes());

				// TODO: 22.06.17 create record scheme without measures
				writer.name(RECORDS_FIELD);
				writeRecords(writer, result.getRecordScheme(), result.getRecords());

				writer.endObject();
			}
			if (reportType.get(1)) {
				writer.name(TOTALS_REPORT);
				writer.beginObject();

				writer.name(MEASURES_FIELD);
				stringListAdapter.write(writer, result.getMeasures());

				writer.name(TOTALS_FIELD);
				writeTotals(writer, result.getRecordScheme(), result.getTotals());

				writer.endObject();
			}
			if (reportType.get(4)) {
				writer.name(RESOLVE_ATTRIBUTES_REPORT);
				writer.beginObject();

				writer.name(FILTER_ATTRIBUTES_FIELD);
				writeFilterAttributes(writer, result.getFilterAttributes());

				writer.endObject();
			}
		}
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

}
