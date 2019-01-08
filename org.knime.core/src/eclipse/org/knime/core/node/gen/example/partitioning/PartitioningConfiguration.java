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
    private final IntegerSetting absoluteSizeInt;

    @Id("absoluteSize")
    @SinceVersion(2)
    @LongConstraints(min = 0)
    private final LongSetting absoluteSizeLong;

    @DoubleConstraints(min = 0, max = 100)
    private final DoubleSetting relativeSize;

    private final EnumSelectionSetting<SamplingMethod> samplingMethod;

    private final ColumnSelectionSetting stratifiedColumn;

    private final BooleanSetting useSeed;

    private final LongSetting seed;

    private final SomeCompositeSetting someCompositeSetting;

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

    @Override
    protected PartitioningConfiguration self() { return this; }

}
