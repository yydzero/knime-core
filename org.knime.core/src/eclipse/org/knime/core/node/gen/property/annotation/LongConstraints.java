package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ ElementType.FIELD })
public @interface LongConstraints {

    /**
     * The minimum value that this property can be set to.
     */
	long min() default Long.MIN_VALUE;

	/**
     * The maximum value that this property can be set to.
     */
	long max() default Long.MAX_VALUE;

}
