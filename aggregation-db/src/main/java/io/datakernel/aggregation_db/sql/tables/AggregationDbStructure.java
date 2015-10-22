/**
 * This class is generated by jOOQ
 */
package io.datakernel.aggregation_db.sql.tables;


import io.datakernel.aggregation_db.sql.DefaultSchema;
import io.datakernel.aggregation_db.sql.Keys;
import io.datakernel.aggregation_db.sql.tables.records.AggregationDbStructureRecord;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@Generated(
	value = {
		"http://www.jooq.org",
		"jOOQ version:3.6.2"
	},
	comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class AggregationDbStructure extends TableImpl<AggregationDbStructureRecord> {

	private static final long serialVersionUID = 426656050;

	/**
	 * The reference instance of <code>aggregation_db_structure</code>
	 */
	public static final AggregationDbStructure AGGREGATION_DB_STRUCTURE = new AggregationDbStructure();

	/**
	 * The class holding records for this type
	 */
	@Override
	public Class<AggregationDbStructureRecord> getRecordType() {
		return AggregationDbStructureRecord.class;
	}

	/**
	 * The column <code>aggregation_db_structure.id</code>.
	 */
	public final TableField<AggregationDbStructureRecord, String> ID = createField("id", org.jooq.impl.SQLDataType.VARCHAR.length(100).nullable(false).defaulted(true), this, "");

	/**
	 * The column <code>aggregation_db_structure.keys</code>.
	 */
	public final TableField<AggregationDbStructureRecord, String> KEYS = createField("keys", org.jooq.impl.SQLDataType.VARCHAR.length(1000).nullable(false).defaulted(true), this, "");

	/**
	 * The column <code>aggregation_db_structure.inputFields</code>.
	 */
	public final TableField<AggregationDbStructureRecord, String> INPUTFIELDS = createField("inputFields", org.jooq.impl.SQLDataType.VARCHAR.length(1000).nullable(false).defaulted(true), this, "");

	/**
	 * The column <code>aggregation_db_structure.outputFields</code>.
	 */
	public final TableField<AggregationDbStructureRecord, String> OUTPUTFIELDS = createField("outputFields", org.jooq.impl.SQLDataType.VARCHAR.length(1000).nullable(false), this, "");

	/**
	 * The column <code>aggregation_db_structure.predicates</code>.
	 */
	public final TableField<AggregationDbStructureRecord, String> PREDICATES = createField("predicates", org.jooq.impl.SQLDataType.VARCHAR.length(1000), this, "");

	/**
	 * Create a <code>aggregation_db_structure</code> table reference
	 */
	public AggregationDbStructure() {
		this("aggregation_db_structure", null);
	}

	/**
	 * Create an aliased <code>aggregation_db_structure</code> table reference
	 */
	public AggregationDbStructure(String alias) {
		this(alias, AGGREGATION_DB_STRUCTURE);
	}

	private AggregationDbStructure(String alias, Table<AggregationDbStructureRecord> aliased) {
		this(alias, aliased, null);
	}

	private AggregationDbStructure(String alias, Table<AggregationDbStructureRecord> aliased, Field<?>[] parameters) {
		super(alias, DefaultSchema.DEFAULT_SCHEMA, aliased, parameters, "");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public UniqueKey<AggregationDbStructureRecord> getPrimaryKey() {
		return Keys.KEY_AGGREGATION_DB_STRUCTURE_PRIMARY;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<UniqueKey<AggregationDbStructureRecord>> getKeys() {
		return Arrays.<UniqueKey<AggregationDbStructureRecord>>asList(Keys.KEY_AGGREGATION_DB_STRUCTURE_PRIMARY);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AggregationDbStructure as(String alias) {
		return new AggregationDbStructure(alias, this);
	}

	/**
	 * Rename this table
	 */
	public AggregationDbStructure rename(String name) {
		return new AggregationDbStructure(name, null);
	}
}
