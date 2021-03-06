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

final class ExpressionArraySet implements Expression {
	private final Expression array;
	private final Expression position;
	private final Expression newElement;

	ExpressionArraySet(Expression array, Expression position, Expression newElement) {
		this.array = checkNotNull(array);
		this.position = checkNotNull(position);
		this.newElement = checkNotNull(newElement);
	}

	@Override
	public Type type(Context ctx) {
		return Type.VOID_TYPE;
	}

	@Override
	public Type load(Context ctx) {
		GeneratorAdapter g = ctx.getGeneratorAdapter();

		array.load(ctx);
		position.load(ctx);
		newElement.load(ctx);
		g.arrayStore(getType(array.type(ctx).getDescriptor().substring(1)));
		return Type.VOID_TYPE;
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ExpressionArraySet that = (ExpressionArraySet) o;

		if (!array.equals(that.array)) return false;
		if (!position.equals(that.position)) return false;
		if (!newElement.equals(that.newElement)) return false;

		return true;

	}

	@Override
	public int hashCode() {
		int result = array.hashCode();
		result = 31 * result + position.hashCode();
		result = 31 * result + newElement.hashCode();
		return result;
	}
}
