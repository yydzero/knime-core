package org.knime.core.data.container.table;

import org.knime.core.data.RowKey;
import org.knime.core.data.table.WriteValue;

/**
 * TODO
 *
 * @author dietzc
 */
public interface RowKeyWriteValue extends WriteValue {
    /**
     * TODO
     *
     * @param key
     */
    void setRowKey(RowKey key);
}