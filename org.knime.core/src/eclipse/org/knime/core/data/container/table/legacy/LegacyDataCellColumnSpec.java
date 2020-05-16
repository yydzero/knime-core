/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 17, 2020 (dietzc): created
 */
package org.knime.core.data.container.table.legacy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellDataInput;
import org.knime.core.data.DataCellDataOutput;
import org.knime.core.data.DataCellSerializer;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingCell;
import org.knime.core.data.container.LongUTFDataInputStream;
import org.knime.core.data.container.LongUTFDataOutputStream;
import org.knime.core.data.container.table.DataCellReadValue;
import org.knime.core.data.container.table.DataCellWriteValue;
import org.knime.core.data.container.table.legacy.LegacyMappings.LegacyDataCellMapper;
import org.knime.core.data.container.table.legacy.LegacyMappings.LegacyMappingInfo;
import org.knime.core.data.container.table.legacy.LegacySchemaMapping.DataCellSerMap;
import org.knime.core.data.table.ColumnSpec;
import org.knime.core.data.table.access.AbstractColumnChunkAccess;
import org.knime.core.data.table.access.ColumnChunkAccess;
import org.knime.core.data.table.store.ColumnChunk;
import org.knime.core.data.table.store.ColumnChunkSpec;
import org.knime.core.data.table.store.types.BinarySupplChunk;
import org.knime.core.data.table.store.types.VarBinaryChunk;
import org.knime.core.util.Pair;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz
 * @param <C>
 */
class LegacyDataCellColumnSpec<C extends ColumnChunk> implements ColumnSpec<BinarySupplChunk<C>> {

    private static final CharsetDecoder DECODER = Charset.forName("UTF-8").newDecoder()
        .onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

    private static final CharsetEncoder ENCODER = Charset.forName("UTF-8").newEncoder()
        .onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

    private LegacyMappingInfo<C> m_info;

    private DataCellSerMap m_serMap;

    public LegacyDataCellColumnSpec(final DataCellSerMap serMap, final LegacyMappingInfo<C> info) {
        m_info = info;
        m_serMap = serMap;
    }

    @Override
    public ColumnChunkSpec<BinarySupplChunk<C>> getColumnChunkSpec() {
        return new BinarySupplChunk.BinarySupplChunkSpec<>(m_info.getColumnChunkSpec());
    }

    @Override
    public ColumnChunkAccess<BinarySupplChunk<C>> createAccess() {
        return new DataCellColumnAccess<>(m_serMap, m_info);
    }

    static class DataCellColumnAccess<C extends ColumnChunk> extends AbstractColumnChunkAccess<BinarySupplChunk<C>>
        implements DataCellReadValue, DataCellWriteValue {

        private VarBinaryChunk m_binaryData;

        private C m_valueData;

        private final Class<? extends DataCell> m_cellType;

        private LegacyDataCellMapper<C> m_adapter;

        private final DataCellSerMap m_serMap;

        public DataCellColumnAccess(final DataCellSerMap serMap, final LegacyMappingInfo<C> info) {
            m_serMap = serMap;
            m_cellType = info.getCellType();
            m_adapter = info.getMapper();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void load(final BinarySupplChunk<C> data) {
            super.load(data);
            m_binaryData = data.getSupplementChunk();
            m_valueData = data.getChunk();
        }

        @Override
        public void setDataCell(final DataCell cell) {
            if (cell.getClass() == m_cellType) {
                m_adapter.set(m_index, cell, m_valueData);
            } else if (!cell.isMissing()) {
                // Intentional code duplication to save an additional if check for the default case.
                // we want to write our data here as well for domain calculation
                m_adapter.set(m_index, cell, m_valueData);
                try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
                        DCTableStoreOutputStream stream = new DCTableStoreOutputStream(out, m_serMap)) {
                    stream.writeDataCell(cell);
                    m_binaryData.setBytes(m_index, out.toByteArray());
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                m_valueData.setMissing(m_index);

                final String cause = ((MissingCell)cell).getError();
                if (cause != null) {
                    try {
                        final ByteBuffer encoded = ENCODER.encode(CharBuffer.wrap(cause.toCharArray()));
                        m_binaryData.setBytes(m_index, encoded.array());
                    } catch (CharacterCodingException ex) {
                    }
                } else {
                    m_binaryData.setMissing(m_index);
                }
            }
        }

        @Override
        public DataCell getDataCell() {
            if (!m_valueData.isMissing(m_index)) {
                if (m_binaryData.isMissing(m_index)) {
                    return m_adapter.get(m_index, m_valueData);
                } else {
                    try (ByteArrayInputStream out = new ByteArrayInputStream(m_binaryData.getBytes(m_index));
                            final DCTableStoreInputStream stream = new DCTableStoreInputStream(out, m_serMap)) {
                        return stream.readDataCell();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            } else if (!m_binaryData.isMissing(m_index)) {
                try {
                    return new MissingCell(DECODER.decode(ByteBuffer.wrap(m_binaryData.getBytes(m_index))).toString());
                } catch (CharacterCodingException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                return DataType.getMissingCell();
            }
        }
    }

    // Simple helpers
    static class DCTableStoreInputStream extends LongUTFDataInputStream implements DataCellDataInput {
        private final DataCellSerMap m_serInfo;

        DCTableStoreInputStream(final ByteArrayInputStream in, final DataCellSerMap serInfo) {
            super(new DataInputStream(in));
            m_serInfo = serInfo;
        }

        @Override
        public DataCell readDataCell() throws IOException {
            return m_serInfo.getSerializerByIdx(readByte()).deserialize(this);
        }
    }

    static class DCTableStoreOutputStream extends LongUTFDataOutputStream implements DataCellDataOutput {
        private final DataCellSerMap m_serInfo;

        DCTableStoreOutputStream(final ByteArrayOutputStream out, final DataCellSerMap serInfo) {
            super(new DataOutputStream(out));
            m_serInfo = serInfo;
        }

        @Override
        public void writeDataCell(final DataCell cell) throws IOException {
            final Pair<Byte, DataCellSerializer<DataCell>> res = m_serInfo.getSerializer(cell);
            writeByte(res.getFirst());
            res.getSecond().serialize(cell, this);
            flush();
        }
    }
}
