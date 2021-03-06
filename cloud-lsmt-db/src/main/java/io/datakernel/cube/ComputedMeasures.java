/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.aggregation.measure.Measure;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.utils.Primitives;
import org.jetbrains.annotations.Nullable;

import java.util.*;

import static io.datakernel.codegen.Expressions.*;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

public final class ComputedMeasures {
	private ComputedMeasures() {
	}

	public static abstract class AbstractComputedMeasure implements ComputedMeasure {
		protected Set<ComputedMeasure> dependencies;

		public AbstractComputedMeasure(ComputedMeasure... dependencies) {
			this.dependencies = new LinkedHashSet<>(Arrays.asList(dependencies));
		}

		@Nullable
		@Override
		public Class<?> getType(Map<String, Measure> storedMeasures) {
			return null;
		}

		@Override
		public final Set<String> getMeasureDependencies() {
			Set<String> result = new LinkedHashSet<>();
			for (ComputedMeasure dependency : dependencies) {
				result.addAll(dependency.getMeasureDependencies());
			}
			return result;
		}
	}

	private static abstract class AbstractArithmeticMeasure extends AbstractComputedMeasure {
		public AbstractArithmeticMeasure(ComputedMeasure... dependencies) {
			super(dependencies);
		}

		@Override
		public final Class<?> getType(Map<String, Measure> storedMeasures) {
			List<Class<?>> types = new ArrayList<>();
			for (ComputedMeasure dependency : dependencies) {
				types.add(dependency.getType(storedMeasures));
			}
			return Expressions.unifyArithmeticTypes(types);
		}
	}

	public static ComputedMeasure value(Object value) {
		return new ComputedMeasure() {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return Primitives.unwrap(value.getClass());
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return Expressions.value(value);
			}

			@Override
			public Set<String> getMeasureDependencies() {
				return emptySet();
			}
		};
	}

	public static ComputedMeasure measure(String measureId) {
		return new ComputedMeasure() {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return (Class<?>) storedMeasures.get(measureId).getFieldType().getDataType();
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return storedMeasures.get(measureId).valueOfAccumulator(property(record, measureId));
			}

			@Override
			public Set<String> getMeasureDependencies() {
				return singleton(measureId);
			}
		};
	}

	public static ComputedMeasure add(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return Expressions.add(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure sub(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return Expressions.sub(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure div(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractComputedMeasure(measure1, measure2) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				Expression value2 = cast(measure2.getExpression(record, storedMeasures), double.class);
				return Expressions.ifThenElse(Expressions.cmpNe(value2, Expressions.value(0.0)),
						Expressions.div(measure1.getExpression(record, storedMeasures), value2),
						Expressions.value(0.0));
			}
		};
	}

	public static ComputedMeasure mul(ComputedMeasure measure1, ComputedMeasure measure2) {
		return new AbstractArithmeticMeasure(measure1, measure2) {
			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return Expressions.mul(measure1.getExpression(record, storedMeasures), measure2.getExpression(record, storedMeasures));
			}
		};
	}

	public static ComputedMeasure sqrt(ComputedMeasure measure) {
		return new AbstractComputedMeasure(measure) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				Expression value = let(cast(measure.getExpression(record, storedMeasures), double.class));
				return ifThenElse(cmpLe(value, Expressions.value(0.0d)),
						Expressions.value(0.0d),
						callStatic(Math.class, "sqrt", value));
			}
		};
	}

	public static ComputedMeasure sqr(ComputedMeasure measure) {
		return new AbstractComputedMeasure(measure) {
			@Override
			public Class<?> getType(Map<String, Measure> storedMeasures) {
				return double.class;
			}

			@Override
			public Expression getExpression(Expression record, Map<String, Measure> storedMeasures) {
				return let(cast(measure.getExpression(record, storedMeasures), double.class), value ->
						Expressions.mul(value, value));
			}
		};
	}

	public static ComputedMeasure stddev(ComputedMeasure sum, ComputedMeasure sumOfSquares, ComputedMeasure count) {
		return sqrt(variance(sum, sumOfSquares, count));
	}

	public static ComputedMeasure variance(ComputedMeasure sum, ComputedMeasure sumOfSquares, ComputedMeasure count) {
		return sub(div(sumOfSquares, count), sqr(div(sum, count)));
	}

	public static ComputedMeasure percent(ComputedMeasure measure) {
		return mul(measure, value(100));
	}

	public static ComputedMeasure percent(ComputedMeasure numerator, ComputedMeasure denominator) {
		return mul(div(numerator, denominator), value(100));
	}

}
