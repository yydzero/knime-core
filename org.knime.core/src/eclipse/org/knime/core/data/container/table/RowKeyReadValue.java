package org.knime.core.data.container.table;

import org.knime.core.data.RowKey;
import org.knime.core.data.table.ReadValue;

/**
 * TODO
 *
 * @author dietzc
 */
public interface RowKeyReadValue extends ReadValue {
    /**
     * @return TODO
     */
    RowKey getRowKey();
}

