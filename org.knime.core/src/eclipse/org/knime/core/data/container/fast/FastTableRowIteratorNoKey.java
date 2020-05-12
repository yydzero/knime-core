package org.knime.core.data.container.fast;

import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.fast.AdapterRegistry.DataSpecAdapter;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.row.RowReadCursor;

class FastTableRowIteratorNoKey extends CloseableRowIterator {

    private final RowReadCursor m_cursor;

    private final DataCellProducer[] m_producers;

    FastTableRowIteratorNoKey(final ReadTable table, final DataSpecAdapter adapter) {
        m_cursor = table.newCursor();
        m_producers = adapter.createProducers(m_cursor);
    }

    @Override
    public boolean hasNext() {
        return m_cursor.canFwd();
    }

    @Override
    public DataRow next() {
        m_cursor.fwd();
        return new FastTableDataRowNoKey(m_producers);
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

    static class FastTableDataRowNoKey implements DataRow {

        private final DataCellProducer[] m_producer;

        public FastTableDataRowNoKey(final DataCellProducer[] producer) {
            m_producer = producer;
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new Iterator<DataCell>() {
                int idx = 0;

                @Override
                public boolean hasNext() {
                    return idx < m_producer.length;
                }

                @Override
                public DataCell next() {
                    return getCell(idx++);
                }
            };
        }

        @Override
        public int getNumCells() {
            return m_producer.length;
        }

        @Override
        public RowKey getKey() {
            throw new IllegalStateException("RowKey requested, but not part of table. Implementation error!");
        }

        @Override
        public DataCell getCell(final int index) {
            return m_producer[index].get();
        }
    }
}