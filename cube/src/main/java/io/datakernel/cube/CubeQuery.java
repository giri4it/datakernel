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

package io.datakernel.cube;

import com.google.common.base.Joiner;
import io.datakernel.aggregation.AggregationPredicate;
import io.datakernel.aggregation.AggregationPredicates;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static java.util.Arrays.asList;

public final class CubeQuery {
	private static final Joiner JOINER = Joiner.on(',');
	private List<String> attributes = new ArrayList<>();
	private List<String> measures = new ArrayList<>();
	private AggregationPredicate where = AggregationPredicates.alwaysTrue();
	private AggregationPredicate having = AggregationPredicates.alwaysTrue();
	private Integer limit = null;
	private Integer offset = null;
	private List<Ordering> orderings = new ArrayList<>();

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
	 * <td align="right"> 3 </td> <td  align="left"> measures </td>
	 * </tr>
	 * <tr>
	 * <td align="right"> 4 </td> <td  align="left"> resolve attributes </td>
	 * </tr>
	 * </table>
	 */
	private BitSet reportType = new BitSet(5);

	private CubeQuery() {}

	public static CubeQuery create() {
		return new CubeQuery();
	}

	// region builders
	public CubeQuery withMeasures(List<String> measures) {
		this.measures = measures;
		return this;
	}

	public CubeQuery withMeasures(String... measures) {
		return withMeasures(asList(measures));
	}

	public CubeQuery withAttributes(List<String> attributes) {
		this.attributes = attributes;
		return this;
	}

	public CubeQuery withAttributes(String... attributes) {
		return withAttributes(asList(attributes));
	}

	public CubeQuery withWhere(AggregationPredicate where) {
		this.where = where;
		return this;
	}

	public CubeQuery withHaving(AggregationPredicate predicate) {
		this.having = predicate;
		return this;
	}

	public CubeQuery withOrderings(List<CubeQuery.Ordering> orderings) {
		this.orderings = orderings;
		return this;
	}

	public CubeQuery withOrderingAsc(String field) {
		this.orderings.add(CubeQuery.Ordering.asc(field));
		return this;
	}

	public CubeQuery withOrderingDesc(String field) {
		this.orderings.add(CubeQuery.Ordering.desc(field));
		return this;
	}

	public CubeQuery withOrderings(CubeQuery.Ordering... orderings) {
		return withOrderings(asList(orderings));
	}

	public CubeQuery withLimit(Integer limit) {
		this.limit = limit;
		return this;
	}

	public CubeQuery withOffset(Integer offset) {
		this.offset = offset;
		return this;
	}

	public CubeQuery withReportTypes(BitSet reportTypes) {
		this.reportType = reportTypes;
		return this;
	}

	public CubeQuery withReportType(ReportType reportType) {
		switch (reportType) {
			case METADATA:
				this.reportType.set(0);
				break;
			case TOTALS:
				this.reportType.set(1);
				break;
			case DIMENSIONS:
				this.reportType.set(2);
				break;
			case MEASURES:
				this.reportType.set(3);
				break;
			case RESOLVE_ATTRIBUTES:
				this.reportType.set(4);
				break;
		}
		return this;
	}

	public CubeQuery withReportTypes(List<String> reportTypes) {
		for (String reportType : reportTypes) {
			withReportType(ReportType.fromString(reportType));
		}
		return this;
	}

	// endregion

	// region getters
	public List<String> getAttributes() {
		return attributes;
	}

	public List<String> getMeasures() {
		return measures;
	}

	public AggregationPredicate getWhere() {
		return where;
	}

	public List<CubeQuery.Ordering> getOrderings() {
		return orderings;
	}

	public AggregationPredicate getHaving() {
		return having;
	}

	public Integer getLimit() {
		return limit;
	}

	public Integer getOffset() {
		return offset;
	}

	public BitSet getReportType() {
		return reportType;
	}
	// endregion

	// region helper classes

	/**
	 * Represents a query result ordering. Contains a propertyName name and ordering (ascending or descending).
	 */
	public static final class Ordering {
		private final String field;
		private final boolean desc;

		private Ordering(String field, boolean desc) {
			this.field = field;
			this.desc = desc;
		}

		public static Ordering asc(String field) {
			return new Ordering(field, false);
		}

		public static Ordering desc(String field) {
			return new Ordering(field, true);
		}

		public String getField() {
			return field;
		}

		public boolean isAsc() {
			return !isDesc();
		}

		public boolean isDesc() {
			return desc;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Ordering that = (Ordering) o;

			if (desc != that.desc) return false;
			if (!field.equals(that.field)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = field.hashCode();
			result = 31 * result + (desc ? 1 : 0);
			return result;
		}

		@Override
		public String toString() {
			return field + " " + (desc ? "desc" : "asc");
		}
	}

	public enum ReportType {
		METADATA("metadata"),
		TOTALS("totals"),
		DIMENSIONS("dimensions"),
		MEASURES("measures"),
		RESOLVE_ATTRIBUTES("resolveAttributes");

		private String reportType;

		ReportType(String reportType) {
			this.reportType = reportType;
		}

		static ReportType fromString(String reportType) {
			for (ReportType type : ReportType.values()) {
				if (reportType.equals(type.reportType))
					return type;
			}
			throw new IllegalArgumentException(String.format("Unexpected reportType '%s'. Available report types are: %s",
					reportType, JOINER.join(ReportType.values())));
		}
	}
	// endregion

	@Override
	public String toString() {
		return "CubeQuery{" +
				"attributes=" + attributes +
				", measures=" + measures +
				", where=" + where +
				", having=" + having +
				", limit=" + limit +
				", offset=" + offset +
				", orderings=" + orderings +
				", reportType=" + reportType + '}';
	}
}
