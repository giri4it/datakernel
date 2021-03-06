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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;
import io.datakernel.serializer.util.BinaryOutputUtils;

import static io.datakernel.codegen.Expressions.*;

public final class SerializerGenBoolean extends SerializerGenPrimitive {

	public SerializerGenBoolean() {
		super(boolean.class);
	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		return callStatic(BinaryOutputUtils.class, "writeBoolean", byteArray, off, cast(value, boolean.class));
	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (targetType.isPrimitive())
			return call(arg(0), "readBoolean");
		else
			return cast(call(arg(0), "readBoolean"), Boolean.class);
	}
}
