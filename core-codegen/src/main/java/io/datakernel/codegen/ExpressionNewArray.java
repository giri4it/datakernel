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

package io.datakernel.codegen;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static io.datakernel.util.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.getType;

final class ExpressionNewArray implements Expression {
	private final Class<?> type;
	private final Expression length;

	ExpressionNewArray(Class<?> type, Expression length) {
		this.type = checkNotNull(type);
		this.length = checkNotNull(length);
	}

	@Override
	public Type type(Context ctx) {
		if (getType(type).getSort() == Type.ARRAY) {
			return getType(type);
		} else {
			return getType("[L" + type.getName() + ";");
		}
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();
		length.load(ctx);
		if (getType(type).getSort() == Type.ARRAY) {
			g.newArray(getType(getType(type).getDescriptor().substring(1)));
			return getType(type);
		} else {
			g.newArray(getType(type));
			return getType("[L" + type.getName() + ";");
		}
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionNewArray that = (ExpressionNewArray) o;

		if (!type.equals(that.type)) return false;
		if (!length.equals(that.length)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = type.hashCode();
		result = 31 * result + length.hashCode();
		return result;
	}
}
