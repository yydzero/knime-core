package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ ElementType.FIELD })
public @interface IntegerConstraints {

    /**
     * The minimum value that this property can be set to.
     */
	int min() default Integer.MIN_VALUE;

	/**
     * The maximum value that this property can be set to.
     */
	int max() default Integer.MAX_VALUE;

}
