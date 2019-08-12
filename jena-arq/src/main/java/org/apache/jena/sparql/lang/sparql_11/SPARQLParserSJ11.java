package org.apache.jena.sparql.lang.sparql_11;

import java.io.InputStream;
import java.io.Reader;

public class SPARQLParserSJ11 extends SPARQLParser11{
    /**
     * Constructor with InputStream.
     *
     * @param stream
     */
    public SPARQLParserSJ11(InputStream stream) {
        super(stream);
    }

    /**
     * Constructor with InputStream and supplied encoding
     *
     * @param stream
     * @param encoding
     */
    public SPARQLParserSJ11(InputStream stream, String encoding) {
        super(stream, encoding);
    }

    /**
     * Constructor.
     *
     * @param stream
     */
    public SPARQLParserSJ11(Reader stream) {
        super(stream);
    }

    /**
     * Constructor with generated Token Manager.
     *
     * @param tm
     */
    public SPARQLParserSJ11(SPARQLParser11TokenManager tm) {
        super(tm);
    }
}
