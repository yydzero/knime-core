package org.knime.core.data.container.table.legacy;

import java.io.File;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.RowContainer;
import org.knime.core.data.container.RowContainerFactory;
import org.knime.core.data.table.arrow.ArrowTableChunkStoreFactory;

/**
 * TODO
 *
 * @author dietzc
 */
public class LegacyRowContainerFactory implements RowContainerFactory {

    // TODO
    private final static ArrowTableChunkStoreFactory FACTORY = new ArrowTableChunkStoreFactory();

    @Override
    public boolean supports(final DataTableSpec spec) {
        // TODO extend with "non-primitives"
        for (final DataColumnSpec colSpec : spec) {
            if (LegacyMappings.PRIMITIVE_MAPPINGS.get(colSpec.getType()) == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public RowContainer create(final long tableId, final DataTableSpec spec, final File dest, final boolean isRowKey) {
        return new LegacyRowContainer((int)tableId, spec, dest, FACTORY, isRowKey);
    }
}