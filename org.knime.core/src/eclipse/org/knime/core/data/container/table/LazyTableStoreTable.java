package org.knime.core.data.container.table;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.TimerTask;

import org.knime.core.data.table.TableSchema;
import org.knime.core.data.table.store.TableChunkReadStore;
import org.knime.core.data.table.store.TableChunkStoreFactory;
import org.knime.core.internal.ReferencedFile;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.util.KNIMETimer;

/**
 * Fast table which is lazily loaded. Similar to 'ContainerTable' with delayed 'CopyTask'
 *
 * @author Christian Dietz, KNIME GmbH
 * @since 4.2
 */
public class LazyTableStoreTable extends AbstractFastTable {

    private AccessTask m_readTask;

    private TableChunkReadStore m_store;

    /**
     * TODO
     *
     * @param context
     * @throws InvalidSettingsException
     */
    public LazyTableStoreTable(final LoadContext context) throws InvalidSettingsException {
        super(-1, context.getTableSpec(), null, context.getSettings().getLong(FAST_TABLE_CONTAINER_SIZE));

        final NodeSettingsRO settings = context.getSettings();
        final TableSchemaMapping mapping = FastTableUtil.createInstance(settings.getString(FAST_TABLE_MAPPING_TYPE));
        mapping.loadFrom(context.getTableSpec(), settings.getNodeSettings(FAST_TABLE_SCHEMA));

        // TODO access table store factory from central registry to avoid several root contexts.
        m_readTask = new AccessTask(context.getDataFileRef(),
            FastTableUtil.createInstance(context.getSettings().getString(FAST_TABLE_CONTAINER_TYPE)),
            mapping.getSchema());
    }

    @Override
    public void ensureOpen() {
        // TODO revise logic here, especially sync logic
        AccessTask readTask = m_readTask;
        if (readTask == null) {
            return;
        }
        synchronized (m_readTask) {
            // synchronized may have blocked when another thread was
            // executing the copy task. If so, there is nothing else to
            // do here
            if (m_readTask == null) {
                return;
            }
            m_store = m_readTask.createTableStore();
            m_readTask = null;
        }
    }

    @Override
    public void clear() {
        try {
            // just close the store. no need to clean anything up
            // TODO null check required?
            if (m_store != null) {
                m_store.close();
            }
        } catch (Exception ex) {
        }
    }

    @Override
    public TableChunkReadStore getStore() {
        ensureOpen();
        return m_store;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveToFileOverwrite(final File f, final NodeSettingsWO settings, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        throw new IllegalStateException("Lazy fast tables have already been saved. This is an implementation error!");
    }

    final static class AccessTask {

        private final NodeLogger LOGGER = NodeLogger.getLogger(AccessTask.class);

        private final ReferencedFile m_fileRef;

        private final TableChunkStoreFactory m_factory;

        private final TableSchema m_schema;

        /**
         * Delay im ms until copying process is reported to LOGGER, small files won't report their copying (if faster
         * than this threshold).
         */
        private static final long NOTIFICATION_DELAY = 3000;

        AccessTask(final ReferencedFile file, final TableChunkStoreFactory factory, final TableSchema schema) {
            m_schema = schema;
            m_factory = factory;
            m_fileRef = file;
        }

        TableChunkReadStore createTableStore() {
            // timer task which prints a INFO message that the copying
            // is in progress.
            TimerTask timerTask = null;
            m_fileRef.lock();
            try {
                final File file = m_fileRef.getFile();
                timerTask = new TimerTask() {
                    /** {@inheritDoc} */
                    @Override
                    public void run() {
                        double sizeInMB = file.length() / (double)(1 << 20);
                        String size = NumberFormat.getInstance().format(sizeInMB);
                        LOGGER.debug(
                            "Extracting data file \"" + file.getAbsolutePath() + "\" to temp dir (" + size + "MB)");
                    }
                };
                KNIMETimer.getInstance().schedule(timerTask, NOTIFICATION_DELAY);

                // TODO why do we have to copy if we make sure we don't delete?
                return m_factory.createReadStore(m_schema, file);
            } catch (Exception ex) {
                throw new RuntimeException(
                    "Exception while accessing file: \"" + m_fileRef.getFile().getName() + "\": " + ex.getMessage(),
                    ex);
            } finally {
                if (timerTask != null) {
                    timerTask.cancel();
                }
                m_fileRef.unlock();
            }
        }
    }
}