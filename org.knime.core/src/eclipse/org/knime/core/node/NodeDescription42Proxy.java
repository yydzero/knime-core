/*
 * ------------------------------------------------------------------------
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
 * Created on 5.10.2019 by Mark Ortmann
 */
package org.knime.core.node;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.xmlbeans.XmlCursor;
import org.apache.xmlbeans.XmlError;
import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.apache.xmlbeans.XmlOptions;
import org.knime.core.node.NodeFactory.NodeType;
import org.knime.core.node.context.ports.ExchangeablePortGroup;
import org.knime.core.node.context.ports.ExtendablePortGroup;
import org.knime.core.node.context.ports.ModifiablePortsConfiguration;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.node.v42.DynPortGrp;
import org.knime.node.v42.ExPortGrp;
import org.knime.node.v42.InPort;
import org.knime.node.v42.KnimeNode;
import org.knime.node.v42.KnimeNodeDocument;
import org.knime.node.v42.ModifiablePort;
import org.knime.node.v42.ModifiablePortGrp;
import org.knime.node.v42.OutPort;
import org.knime.node.v42.Port;
import org.knime.node.v42.Ports;
import org.knime.node.v42.TypedModifiablePort;
import org.knime.node.v42.View;
import org.knime.node.v42.Views;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Implementation of {@link NodeDescription} for node descriptions introduced with 4.2. It uses XMLBeans to extract the
 * information from the XML file.<br>
 * If assertions are enabled (see {@link KNIMEConstants#ASSERTIONS_ENABLED} it also checks the contents of the XML for
 * against the XML schema and reports errors via the logger. This version of the node description supports nodes with
 * modifiable ports.
 *
 * @author Mark Ortmann, KNIME GmbH, Berlin, Germany
 * @since 4.2
 */
public final class NodeDescription42Proxy extends NodeDescription {

    private static final XmlOptions OPTIONS = new XmlOptions();

    private static final NodeLogger logger = NodeLogger.getLogger(NodeDescription42Proxy.class);

    private static final String SCHEMA_VIOLATION_MSG =
        "Node description of '%s' does not conform to the Schema. Violations follow.";

    static {
        Map<String, String> namespaceMap = new HashMap<String, String>(1);
        namespaceMap.put("", KnimeNodeDocument.type.getContentModel().getName().getNamespaceURI());
        OPTIONS.setLoadSubstituteNamespaces(namespaceMap);
    }

    private final KnimeNodeDocument m_document;

    /**
     * Creates a new proxy object using the given XML document. If assertions are enabled (see
     * {@link KNIMEConstants#ASSERTIONS_ENABLED} it also checks the contents of the XML for against the XML schema and
     * reports errors via the logger.
     *
     * @param doc the XML document of the node description XML file
     * @throws XmlException if something goes wrong while analyzing the XML structure
     */
    public NodeDescription42Proxy(final Document doc) throws XmlException {
        m_document = KnimeNodeDocument.Factory.parse(doc.getDocumentElement(), OPTIONS);
        setIsDeprecated(m_document.getKnimeNode().getDeprecated());
        validate();
        addPortDescriptions(null);
    }

    /**
     * Creates a new proxy object using the given knime node document. If assertions are enabled (see
     * {@link KNIMEConstants#ASSERTIONS_ENABLED} it also checks the contents of the XML for against the XML schema and
     * reports errors via the logger.
     *
     * @param doc a knime node document
     */
    public NodeDescription42Proxy(final KnimeNodeDocument doc) {
        this(doc, KNIMEConstants.ASSERTIONS_ENABLED, null);
    }

    /**
     * Creates a new proxy object using the given knime node document. If assertions are enabled (see
     * {@link KNIMEConstants#ASSERTIONS_ENABLED} it also checks the contents of the XML for against the XML schema and
     * reports errors via the logger.
     *
     * @param doc a knime node document
     * @param validate flag indicating whether or not toe validate the document
     * @param portsConfiguration
     */
    private NodeDescription42Proxy(final KnimeNodeDocument doc, final boolean validate,
        final ModifiablePortsConfiguration portsConfiguration) {
        m_document = doc;
        setIsDeprecated(m_document.getKnimeNode().getDeprecated());
        if (validate) {
            validate();
        }
        clearPorts();
        addPortDescriptions(portsConfiguration);
    }

    /**
     * Validate against the XML Schema. If violations are found they are reported via the logger as coding problems.
     *
     * @return <code>true</code> if the document is valid, <code>false</code> otherwise
     */
    protected final boolean validate() {
        // this method has default visibility so that we can use it in testcases
        XmlOptions options = new XmlOptions(OPTIONS);
        List<XmlError> errors = new ArrayList<XmlError>();
        options.setErrorListener(errors);
        boolean valid = m_document.validate(options);
        if (!valid) {
            logger.coding(String.format(SCHEMA_VIOLATION_MSG, getNodeName()));
            for (XmlError err : errors) {
                logger.coding(err.toString());
            }
        }

        return validateInsertionIndices(valid);
    }

    private boolean validateInsertionIndices(final boolean valid) {
        //        final Ports ports = m_document.getKnimeNode().getPorts();
        //        ports.getDynInOutPortArray()
        //        if (!validInsertionIndices(ports.getInPortList(), ports.getDynInPortList())) {
        //            if (valid) {
        //                logger.coding(String.format(SCHEMA_VIOLATION_MSG, m_document.getKnimeNode().getName()));
        //                valid = false;
        //            }
        //            logger.coding("The dynInPort insert-before attribute can at most the maximum inPort index plus one.");
        //        }
        //
        //        if (!validInsertionIndices(ports.getOutPortList(), ports.getDynOutPortList())) {
        //            if (valid) {
        //                logger.coding(String.format(SCHEMA_VIOLATION_MSG, getNodeName()));
        //                valid = false;
        //            }
        //            logger.coding("The dynOutPort insert-before attribute can at most the maximum outPort index plus one.");
        //        }
        //        return valid;
        // TODO: change this
        return true;
    }

    //    private static boolean validInsertionIndices(final List<? extends Port> ports,
    //        final List<? extends DynPort> dynPorts) {
    //        return dynPorts.stream().mapToLong(p -> p.getInsertBefore().longValue()).max()
    //            .orElse(0) <= ports.stream().mapToLong(i -> i.getIndex().longValue()).max().orElse(-1) + 1;
    //    }

    @Override
    public String getIconPath() {
        return m_document.getKnimeNode().getIcon();
    }

    @Override
    public String getInportDescription(final int index) {
        if (m_document.getKnimeNode().getPorts() == null) {
            return null;
        }

        for (InPort inPort : m_document.getKnimeNode().getPorts().getInPortList()) {
            if (inPort.getIndex().intValue() == index) {
                return stripXmlFragment(inPort);
            }
        }
        return null;
    }

    @Override
    public String getInportName(final int index) {
        if (m_document.getKnimeNode().getPorts() == null) {
            return null;
        }

        for (InPort inPort : m_document.getKnimeNode().getPorts().getInPortList()) {
            if (inPort.getIndex().intValue() == index) {
                return inPort.getName();
            }
        }
        return null;
    }

    @Override
    public String getInteractiveViewName() {
        if (m_document.getKnimeNode().getInteractiveView() != null) {
            return m_document.getKnimeNode().getInteractiveView().getName();
        } else {
            return null;
        }
    }

    @Override
    public String getNodeName() {
        String nodeName = m_document.getKnimeNode().getName();
        if (m_document.getKnimeNode().getDeprecated() && !nodeName.matches("^.+\\s+\\(?[dD]eprecated\\)?$")) {
            return nodeName + " (deprecated)";
        } else {
            return nodeName;
        }
    }

    @Override
    public String getOutportDescription(final int index) {
        if (m_document.getKnimeNode().getPorts() == null) {
            return null;
        }

        for (OutPort outPort : m_document.getKnimeNode().getPorts().getOutPortList()) {
            if (outPort.getIndex().intValue() == index) {
                return stripXmlFragment(outPort);
            }
        }
        return null;
    }

    @Override
    public String getOutportName(final int index) {
        if (m_document.getKnimeNode().getPorts() == null) {
            return null;
        }

        for (OutPort outPort : m_document.getKnimeNode().getPorts().getOutPortList()) {
            if (outPort.getIndex().intValue() == index) {
                return outPort.getName();
            }
        }
        return null;
    }

    @Override
    public NodeType getType() {
        try {
            return NodeType.valueOf(m_document.getKnimeNode().getType().toString());
        } catch (IllegalArgumentException ex) {
            logger.error("Unknown node type for " + m_document.getKnimeNode().getName() + ": "
                + m_document.getKnimeNode().getDomNode().getAttributes().getNamedItem("type").getNodeValue(), ex);
            return NodeType.Unknown;
        }
    }

    @Override
    public int getViewCount() {
        Views views = m_document.getKnimeNode().getViews();
        return (views == null) ? 0 : views.sizeOfViewArray();
    }

    @Override
    public String getViewDescription(final int index) {
        if (m_document.getKnimeNode().getViews() == null) {
            return null;
        }

        for (View view : m_document.getKnimeNode().getViews().getViewList()) {
            if (view.getIndex().intValue() == index) {
                return stripXmlFragment(view);
            }
        }
        return null;
    }

    @Override
    public String getViewName(final int index) {
        if (m_document.getKnimeNode().getViews() == null) {
            return null;
        }

        for (View view : m_document.getKnimeNode().getViews().getViewList()) {
            if (view.getIndex().intValue() == index) {
                return view.getName();
            }
        }
        return null;
    }

    @Override
    protected void setIsDeprecated(final boolean b) {
        super.setIsDeprecated(b);
        m_document.getKnimeNode().setDeprecated(b);
    }

    @Override
    public Element getXMLDescription() {
        return (Element)m_document.getKnimeNode().getDomNode();
    }

    @Override
    NodeDescription createUpdatedNodeDescription(final ModifiablePortsConfiguration portsConfiguration) {
        return new NodeDescription42Proxy((KnimeNodeDocument)m_document.copy(), false, portsConfiguration);
    }

    /**
     * Clears all entries that are shown by the node documentation.
     */
    private void clearPorts() {
        final Ports ports = m_document.getKnimeNode().getPorts();
        ports.getInPortList().clear();
        ports.getOutPortList().clear();
    }

    private void addPortDescriptions(final ModifiablePortsConfiguration portsConfiguration) {
        final KnimeNode knimeNode = m_document.getKnimeNode();
        final Ports ports = knimeNode.getPorts();
        // create input port descriptions
        // TODO: fix
        addPorts(portsConfiguration, ports, true, Ports::getFixInPortList, Ports::addNewInPort,
            portsConfiguration != null ? portsConfiguration.getInputPorts() : null);
        // create output port descriptions
        addPorts(portsConfiguration, ports, false, Ports::getFixOutPortList, Ports::addNewOutPort,
            portsConfiguration != null ? portsConfiguration.getOutputPorts() : null);
    }

    private static void addPorts(final ModifiablePortsConfiguration portsConfiguration, final Ports ports,
        final boolean addInPorts, final Function<Ports, List<? extends Port>> getFixPorts,
        final Function<Ports, Port> createStdPort, final PortType[] portTypes) {
        List<AdapterPort> adapterPorts = new ArrayList<>();
        for (final Port p : getFixPorts.apply(ports)) {
            adapterPorts.add(new AdapterPort(p));
        }
        if (portsConfiguration != null) {
            final Map<String, ExtendablePortGroup> extendablePorts = portsConfiguration.getExtendablePorts();
            for (final DynPortGrp dynPort : ports.getDynPortGrpList()) {
                final PortType[] configuredPorts =
                    extendablePorts.get(dynPort.getGroupIdentifier()).getConfiguredPorts();
                if (configuredPorts.length > 0 && accept(addInPorts, dynPort)) {
                    for (int i = 0; i < configuredPorts.length; i++) {
                        adapterPorts.add(new AdapterPort(dynPort, configuredPorts[i], addInPorts));
                    }
                }
            }
            final Map<String, ExchangeablePortGroup> exchangeablePorts = portsConfiguration.getExchangeablePorts();
            for (final ExPortGrp extPort : ports.getExPortGrpList()) {
                if (accept(addInPorts, extPort)) {
                    adapterPorts.add(new AdapterPort(extPort,
                        exchangeablePorts.get(extPort.getGroupIdentifier()).getSelectedPortType(), addInPorts));
                }
            }
        }
        createStdPorts(adapterPorts, ports, createStdPort, portTypes);
    }

    private static boolean accept(final boolean addInPorts, final ModifiablePortGrp extPort) {
        return addInPorts ? isInPort(extPort) : isOutPort(extPort);
    }

    private static boolean isInPort(final ModifiablePortGrp p) {
        return p.sizeOfTypedModInPortArray() > 0 || p.isSetDefaultInPort();
    }

    private static boolean isOutPort(final ModifiablePortGrp p) {
        return p.sizeOfTypedModOutPortArray() > 0 || p.isSetDefaultOutPort();
    }

    private static void createStdPorts(final List<AdapterPort> adapterPorts, final Ports ports,
        final Function<Ports, Port> createStdPort, final PortType[] portTypes) {
        Collections.sort(adapterPorts);
        int idx = 0;
        for (final AdapterPort p : adapterPorts) {
            Port port = createStdPort.apply(ports);
            port.set(p.getXmlObject());
            addPortColor(port, portTypes != null ? portTypes[idx] : BufferedDataTable.TYPE, p.getSuffix());
            port.setIndex(BigInteger.valueOf(idx++));
            port.setName(p.getName());
        }
    }

    private static void addPortColor(final Port port, final PortType portType, final String suffix) {
        XmlCursor cursor = port.newCursor();
        cursor.toNextToken();
        while (cursor.isAnyAttr()) {
            cursor.toNextToken();
        }
        cursor.beginElement("svg");
        cursor.insertAttributeWithValue("height", "9");
        cursor.insertAttributeWithValue("width", "10");
        drawElement(cursor, portType);
        cursor.toEndToken();
        cursor.toNextToken();
        cursor.insertElementWithText("i", "Type: " + portType.getName());
        if (!suffix.isEmpty()) {
            cursor.insertElementWithText("b", suffix);
        }
        cursor.insertElement("br");
        cursor.dispose();
    }

    private static void drawElement(final XmlCursor newCursor, final PortType portType) {
        if (portType.equals(BufferedDataTable.TYPE)) {
            newCursor.beginElement("polygon");
            newCursor.insertAttributeWithValue("points", "0,0,0,10,8,5");
            newCursor.insertAttributeWithValue("style", "fill:black,stroke:black,stroke-width:1");
        } else if (portType.equals(FlowVariablePortObject.TYPE)) {
            newCursor.beginElement("circle");
            newCursor.insertAttributeWithValue("cx", "5");
            newCursor.insertAttributeWithValue("cy", "5");
            newCursor.insertAttributeWithValue("r", "4");
            newCursor.insertAttributeWithValue("stroke", "red");
            newCursor.insertAttributeWithValue("stroke-width", "1");
            newCursor.insertAttributeWithValue("fill", "red");
        } else {
            newCursor.beginElement("polygon");
            newCursor.insertAttributeWithValue("points", "0,0,0,10,10,10,10,0");
            newCursor.insertAttributeWithValue("stroke-width", "1");
            newCursor.insertAttributeWithValue("fill", "#" + Integer.toHexString(portType.getColor()));
            newCursor.insertAttributeWithValue("stroke", "#" + Integer.toHexString(portType.getColor()));
        }
        newCursor.toEndToken();
        newCursor.toNextToken();
    }

    private static class AdapterPort implements Comparable<AdapterPort> {

        /** The dynamic port suffix. */
        private static final String DYNAMIC_PORT_SUFFIX = " (dynamic)";

        private static final String EXCHANGEABLE_PORT_SUFFIX = " (exchangeable)";

        private final double m_pos;

        private final String m_name;

        private final String m_desc;

        private final String m_suffix;

        private final XmlObject m_xmlObj;

        AdapterPort(final Port p) {
            m_pos = p.getIndex().intValue();
            m_name = p.getName();
            m_desc = p.newCursor().getTextValue();
            m_suffix = "";
            m_xmlObj = p.copy();
        }

        private AdapterPort(final List<? extends TypedModifiablePort> pList, final ModifiablePort defPort,
            final PortType pType, final BigInteger idx, final String suffix) {
            final ModifiablePort port = pList.stream()//
                .filter(modPort -> pType.getName().equals(modPort.getPortType()))//
                .findFirst()//
                .map(modPort -> (ModifiablePort)modPort)//
                .orElse(defPort);
            m_pos = idx.intValue();
            m_name = port.getName();
            m_desc = port.newCursor().getTextValue();
            m_suffix = suffix;
            m_xmlObj = port.copy();
        }

        AdapterPort(final DynPortGrp p, final PortType pType, final boolean isInPort) {
            this(isInPort ? p.getTypedModInPortList() : p.getTypedModOutPortList(),
                isInPort ? p.getDefaultInPort() : p.getDefaultOutPort(), pType, p.getInsertBefore(),
                DYNAMIC_PORT_SUFFIX);
        }

        AdapterPort(final ExPortGrp p, final PortType pType, final boolean isInPort) {
            this(isInPort ? p.getTypedModInPortList() : p.getTypedModOutPortList(),
                isInPort ? p.getDefaultInPort() : p.getDefaultOutPort(), pType, p.getIndex(), EXCHANGEABLE_PORT_SUFFIX);
        }

        String getDescription() {
            return m_desc;
        }

        String getName() {
            return m_name;
        }

        String getSuffix() {
            return m_suffix;
        }

        XmlObject getXmlObject() {
            return m_xmlObj;
        }

        @Override
        public int compareTo(final AdapterPort o) {
            return Double.compare(m_pos, o.m_pos);
        }

    }
}
