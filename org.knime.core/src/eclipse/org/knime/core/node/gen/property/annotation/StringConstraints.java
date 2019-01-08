package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ElementType.FIELD})
public @interface StringConstraints {

    /**
     * The maximum allowed length of the annotated String property.
     *
     * @return
     */
    int maxLength() default Integer.MAX_VALUE;

    String regexPattern() default ".*";

}
