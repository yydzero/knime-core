package org.knime.core.node.gen.example.partitioning;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.gen.property.annotation.DoubleConstraints;
import org.knime.core.node.gen.property.annotation.Id;
import org.knime.core.node.gen.property.annotation.IntegerConstraints;
import org.knime.core.node.gen.property.annotation.LongConstraints;
import org.knime.core.node.gen.property.annotation.SinceVersion;
import org.knime.core.node.gen.property.annotation.UntilVersion;
import org.knime.core.node.gen.property.annotation.Version;
import org.knime.core.node.gen.setting.BooleanSetting;
import org.knime.core.node.gen.setting.ColumnSelectionSetting;
import org.knime.core.node.gen.setting.CompositeSetting;
import org.knime.core.node.gen.setting.DoubleSetting;
import org.knime.core.node.gen.setting.EnumSelectionSetting;
import org.knime.core.node.gen.setting.IntegerSetting;
import org.knime.core.node.gen.setting.LongSetting;

@SuppressWarnings({"javadoc", "unused"})
@Version(2)
public class PartitioningConfiguration extends CompositeSetting<PartitioningConfiguration> {

    static enum PartitioningMethod {
            ABSOLUTE, RELATIVE
    }

    static enum SamplingMethod {
            TOP, LINEAR, RANDOM, STRATIFIED
    }

    private static class SomeCompositeSetting extends CompositeSetting<SomeCompositeSetting> {
        private final IntegerSetting someInt;

        public SomeCompositeSetting() {
            someInt = new IntegerSetting(42);
        }

        @Override
        protected SomeCompositeSetting self() { return this; }
    }

    private final EnumSelectionSetting<PartitioningMethod> partitioningMethod;

    @Id("absoluteSize")
    @UntilVersion(1)
    @IntegerConstraints(min = 0)
    //TODO: CD: not a big fan of using custom classes here; int absoluteSizeInt would be so much easier;
    //          however, then we'd have to explicitly annotate every setting (which could be a good thing)
    private final IntegerSetting absoluteSizeInt;

    @Id("absoluteSize")
    @SinceVersion(2)
    //TODO: CD: not a fan of these annotation stacks; would be nicer to merge these annotations into a single one
    @LongConstraints(min = 0)
    private final LongSetting absoluteSizeLong;

    @DoubleConstraints(min = 0, max = 100)
    private final DoubleSetting relativeSize;

    private final EnumSelectionSetting<SamplingMethod> samplingMethod;

    private final ColumnSelectionSetting stratifiedColumn;

    //TODO CD: if we switch back to a purely annotation-based framework, the choices of a string selection could be placed into a factory method that is referenced in the annotations

    private final BooleanSetting useSeed;

    private final LongSetting seed;

    private final SomeCompositeSetting someCompositeSetting;

    //TODO: CD: these will have to be constructed before the spec is known; instead, have a method configure(DataTableSpec[]) in the top-level interface
    public PartitioningConfiguration(final DataTableSpec[] spec) {
        partitioningMethod = new EnumSelectionSetting<>(PartitioningMethod.ABSOLUTE, PartitioningMethod.values());
        absoluteSizeInt = new IntegerSetting(100);
        absoluteSizeLong = new LongSetting(100l);
        relativeSize = new DoubleSetting(20d);

        samplingMethod = new EnumSelectionSetting<>(SamplingMethod.RANDOM, SamplingMethod.values());
        if (spec.length < 1 || spec[0].getNumColumns() < 1) {
            throw new IllegalArgumentException();
        }
        String[] columnChoices = spec[0].getColumnNames();
        stratifiedColumn = new ColumnSelectionSetting(columnChoices[0], columnChoices);

        useSeed = new BooleanSetting(false);
        seed = new LongSetting(System.nanoTime());

        someCompositeSetting = new SomeCompositeSetting();
    }

    //TODO: CD: This method should not be necessary
    @Override
    protected PartitioningConfiguration self() { return this; }

}
