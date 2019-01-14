package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Whether it is allowed for this parameter to be unspecified / null. Note that primitive types can never be null,
 * so this attribute is not allowed to be set for primitive parameters.
 */
@Target({ElementType.FIELD})
public @interface Nullable {
  // TODO: CD: what does a value assigned to null mean?
  // does it mean that the field in the dialog is disabled?
  // if not, should a disabled field in the dialog be saved as null?
  // if so, when a property is saved as null (or not saved at all since it is null) and it is then loaded, shouldn't it be replaced with it's default value?
}
