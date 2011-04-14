package org.dis.loader;

import org.dis.DataSet;
import org.dis.DatabaseTable;
import org.springframework.core.io.ClassPathResource;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;

/**
 * <p>
 * Loads data set from a classpath resource.
 * </p>
 */
public class XMLFileDataSetLoader implements DataSetLoader {

    private final SAXParser saxParser;

    private String resource;

    public XMLFileDataSetLoader(String resource) {
        try {
            this.resource = resource;
            saxParser = SAXParserFactory.newInstance().newSAXParser();
        } catch (ParserConfigurationException e) {
            throw new DataSetLoaderException("Failed to get a new SAX parser", e);
        } catch (SAXException e) {
            throw new DataSetLoaderException("Failed to get a new SAX parser", e);
        }
    }

    public DataSet load() {
        final ClassPathResource classPathResource = new ClassPathResource(resource);

        final DataSetHandler handler = new DataSetHandler();
        try {
            saxParser.parse(classPathResource.getInputStream(), handler);
        } catch (SAXException e) {
            throw new DataSetLoaderException("Failed to parse resource[" + resource + "] correctly.", e);
        } catch (IOException e) {
            throw new DataSetLoaderException("Failed to load input stream for resource [" + resource + "].", e);
        }
        return handler.getDataSet();
    }

    private static final class DataSetHandler extends DefaultHandler {

        private static final String DATASET_ELEMENT = "dataset";
        private static final String INCLUDE_ELEMENT = "include";

        private DataSet dataSet;

        private SpelExpressionParser parser;

        public DataSet getDataSet() {
            return dataSet;
        }

        @Override
        public void startDocument() {
            dataSet = new DataSet();
            parser = new SpelExpressionParser();
        }

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes attributes) {
            if (INCLUDE_ELEMENT.equals(qName)) {
                String resourceName = attributes.getValue("resource");
                DataSetLoader loader = new XMLFileDataSetLoader(resourceName);
                DataSet included = loader.load();
                dataSet.addRows(included.getRows());
            } else if (!DATASET_ELEMENT.equals(qName)) {
                final DataSet.DataSetRow row = new DataSet.DataSetRow();
                row.setDatabaseTable(DatabaseTable.parse(qName));
                for (int i = 0; i < attributes.getLength(); i++) {
                    final String columnName = attributes.getLocalName(i);
                    final String columnValue = attributes.getValue(i);
                    SpelExpression expr = parser.parseRaw(columnValue);                    
                    row.addColumnValue(columnName, expr.getValue());
                }
                dataSet.addRow(row);
            }
        }
    }
}
