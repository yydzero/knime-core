package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Whether it is allowed for this parameter to be unspecified / null. Note that primitive types can never be null,
 * so this attribute is not allowed to be set for primitive parameters.
 */
@Target({ElementType.FIELD})
public @interface Nullable {

}
