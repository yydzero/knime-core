package org.knime.core.data.container.fast;

import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.UnmaterializedCell;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.fast.AdapterRegistry.DataSpecAdapter;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.row.RowReadCursor;
import org.knime.core.data.table.value.StringReadValue;

import gnu.trove.map.hash.TIntIntHashMap;

class PartialRowIterator extends CloseableRowIterator {

    private final RowReadCursor m_cursor;

    private final StringReadValue m_rowKeySupplier;

    private final DataCellProducer[] m_producers;

    private final TIntIntHashMap m_indexMap;

    private final int m_numCells;

    public PartialRowIterator(final ReadTable table, final DataSpecAdapter adapter, final boolean isRowKey,
        final int[] selected) {
        m_cursor = table.newCursor();
        m_rowKeySupplier = isRowKey ? m_cursor.get(0) : null;
        // TODO use selected
        m_producers = adapter.createProducers(m_cursor, selected);
        m_numCells = (int)table.getNumColumns() - (isRowKey ? 1 : 0);

        // TODO only initialize required suppliers in case of partial table.
        // TODO do mapping once. we can use the same mapping for each reader.
        // TODO which mapping to use? output spec from knime or columntypes spec of store?

        // TODO wrong wrong wrong
        m_indexMap = new TIntIntHashMap(selected.length, 0.5f, -1, -1);
        for (int i = 0; i < selected.length; i++) {
            m_indexMap.put(selected[i], i);
        }
    }

    @Override
    public boolean hasNext() {
        return m_cursor.canFwd();
    }

    @Override
    public DataRow next() {
        m_cursor.fwd();
        return new PartialFastTableDataRow(m_rowKeySupplier, m_producers, m_indexMap, m_numCells);
    }

    @Override
    public void close() {
        try {
            m_cursor.close();
        } catch (Exception e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    // TODO split in with rowkey and without rowkey
    static class PartialFastTableDataRow implements DataRow {

        private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance();

        // TODO
        private final StringReadValue m_rowKeyValue;

        private final DataCellProducer[] m_producers;

        private final TIntIntHashMap m_indexMap;

        private final int m_numCells;

        private final int m_offset;

        public PartialFastTableDataRow(final StringReadValue rowKeySupplier, final DataCellProducer[] selectedSuppliers,
            final TIntIntHashMap indexMap, final int numCells) {
            m_rowKeyValue = rowKeySupplier;
            m_offset = m_rowKeyValue != null ? 1 : 0;
            m_producers = selectedSuppliers;
            m_numCells = numCells;
            m_indexMap = indexMap;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<DataCell> iterator() {
            return new Iterator<DataCell>() {
                int idx = m_offset;

                @Override
                public boolean hasNext() {
                    return idx < m_numCells;
                }

                @Override
                public DataCell next() {
                    return getCell(idx++);
                }
            };
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumCells() {
            return m_numCells;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RowKey getKey() {
            // TODO too expensive? Check per access... :-(
            if (m_offset == 0) {
                throw new IllegalStateException("RowKey requested, but not part of table. Implementation error!");
            }
            return new RowKey(m_rowKeyValue.getStringValue());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataCell getCell(final int index) {
            // TODO this is the cost of the contract, that we keep column indices
            // TODO check if this is really faster than Map<Integer,Supplier>
            final int i = m_indexMap.get(index);
            if (i == -1) {
                return INSTANCE;
            } else {
                return m_producers[index + m_offset].get();
            }
        }
    }
}