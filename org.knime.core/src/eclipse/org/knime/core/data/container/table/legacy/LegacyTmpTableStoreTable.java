package org.knime.core.data.container.table.legacy;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.table.AbstractFastTable;
import org.knime.core.data.container.table.TableSchemaMapping;
import org.knime.core.data.table.store.TableChunkReadStore;
import org.knime.core.data.table.store.TableChunkStore;
import org.knime.core.data.table.store.TableChunkStoreFactory;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeSettingsWO;

class LegacyTmpTableStoreTable extends AbstractFastTable {

    private final TableChunkStore m_store;

    private final TableSchemaMapping m_mapping;

    private final TableChunkStoreFactory m_factory;

    // TODO weird that I need so many things?
    LegacyTmpTableStoreTable(final TableChunkStoreFactory factory, //
        final long tableId, //
        final DataTableSpec spec, //
        final TableChunkStore store, //
        final TableSchemaMapping mapping, //
        final long size) {
        super(tableId, spec, mapping.getSchema(), size);
        m_store = store;
        m_mapping = mapping;
        m_factory = factory;
    }

    @Override
    public TableChunkReadStore getStore() {
        return m_store;
    }

    @Override
    public void clear() {
        try {
            // TODO make sure we do the right thing with 'close'.
            // In this case it really means: destroy all (not only kill memory)
            m_store.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void ensureOpen() {
        // Ensure open only interesting for LazyFastTables...
        throw new IllegalStateException("Why called?");
    }

    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        settings.addString(FAST_TABLE_CONTAINER_TYPE, m_factory.getClass().getName());
        settings.addLong(FAST_TABLE_CONTAINER_SIZE, size());

        // TODO any drawback writing to settings and not into file, e.g. as zip entry?
        settings.addString(FAST_TABLE_MAPPING_TYPE, m_mapping.getClass().getName());
        m_mapping.saveTo(settings.addNodeSettings(FAST_TABLE_SCHEMA));

        // TODO ZIP
        m_store.saveToFile(f);
    }
}