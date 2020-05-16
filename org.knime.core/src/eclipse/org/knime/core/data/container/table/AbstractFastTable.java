package org.knime.core.data.container.table;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.container.table.TableStoreIterators.EmptyRowIterator;
import org.knime.core.data.container.table.TableStoreIterators.EmptyRowIteratorNoKey;
import org.knime.core.data.container.table.TableStoreIterators.PartialRowIterator;
import org.knime.core.data.container.table.TableStoreIterators.PartialRowIteratorNoKey;
import org.knime.core.data.container.table.TableStoreIterators.TableStoreRowIterator;
import org.knime.core.data.container.table.TableStoreIterators.TableStoreRowIteratorNoKey;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableSchema;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.store.TableChunkReadStore;
import org.knime.core.data.table.store.chunk.TableChunkReaderConfig;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.ExtensionTable;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.WorkflowDataRepository;

/**
 * TODO
 *
 * @author Christian Dietz
 */
public abstract class AbstractFastTable extends ExtensionTable implements FastTable {

    protected static final String FAST_TABLE_SCHEMA = "FAST_TABLE_SCHEMA_MAPPER";

    protected static final String FAST_TABLE_CONTAINER_SIZE = "FAST_TABLE_CONTAINER_SIZE";

    protected static final String FAST_TABLE_CONTAINER_TYPE = "FAST_TABLE_CONTAINER_TYPE";

    protected static final String FAST_TABLE_MAPPING_TYPE = "FAST_TABLE_MAPPING_TYPE";

    private final long m_tableId;

    private final DataTableSpec m_spec;

    private final boolean m_isRowKey;

    private final TableSchema m_schema;

    private final long m_size;

    protected AbstractFastTable(final long id, final DataTableSpec spec, final TableSchema schema, final long size) {
        m_spec = spec;
        m_tableId = id;
        m_schema = schema;
        m_isRowKey = schema.getNumColumns() - 1 == spec.getNumColumns();
        m_size = size;
    }

    @Override
    public DataTableSpec getDataTableSpec() {
        return m_spec;
    }

    @Override
    public int getTableId() {
        return (int)m_tableId;
    }

    @Override
    public void putIntoTableRepository(final WorkflowDataRepository dataRepository) {
        // TODO only relevant in case of newly created tables?
        dataRepository.addTable((int)m_tableId, this);
    }

    @Override
    public boolean removeFromTableRepository(final WorkflowDataRepository dataRepository) {
        // TODO only relevant in case of newly created tables?
        dataRepository.removeTable((int)m_tableId);
        return true;
    }

    @SuppressWarnings("resource")
    @Override
    public CloseableRowIterator iterator() {
        final ReadTable read = TableUtils.create(m_schema, getStore());
        if (m_isRowKey) {
            return new TableStoreRowIterator(read);
        } else {
            return new TableStoreRowIteratorNoKey(read);
        }
    }

    @Override
    public CloseableRowIterator iteratorWithFilter(final TableFilter filter, final ExecutionMonitor exec) {
        return iterator(filter, exec);
    }

    @Override
    public void saveToFile(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        settings.addString(CFG_TABLE_IMPL, LazyTableStoreTable.class.getName());
        NodeSettingsWO derivedSettings = settings.addNodeSettings(CFG_TABLE_DERIVED_SETTINGS);
        saveToFileOverwrite(f, derivedSettings, exec);
    }

    @Override
    public long size() {
        return m_size;
    }

    // TODO handle exec!
    @SuppressWarnings("resource")
    private CloseableRowIterator iterator(final TableFilter filter,
        @SuppressWarnings("unused") final ExecutionMonitor exec) {
        final TableChunkReadStore store = getStore();

        // TODO use exec? slow :-(
        // TODO implement row index selection as RowBatchReaderConfig (start at...)
        final Optional<Set<Integer>> materializeColumnIndices = filter.getMaterializeColumnIndices();
        // special case this one!
        if (materializeColumnIndices.isPresent()) {
            final int numSelected = materializeColumnIndices.get().size();
            if (numSelected == 0) {
                return m_isRowKey ? new EmptyRowIterator(TableUtils.create(m_schema, store))
                    : new EmptyRowIteratorNoKey(m_schema.getNumColumns(), m_size);
            } else if (numSelected < m_schema.getNumColumns() - (m_isRowKey ? 1 : 0)) {
                ReadTable table = TableUtils.create(m_schema, store);
                TableChunkReaderConfig config = FastTableUtil.wrap(filter);
                if (m_isRowKey) {
                    return new PartialRowIterator(table, config);
                } else {
                    return new PartialRowIteratorNoKey(table, config);
                }
            }
        }
        return iterator();
    }
}
