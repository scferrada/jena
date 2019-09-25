package org.apache.jena.sparql.lang;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.SimJoinQuery;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.Var;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParserSPARQLSJ11 extends ParserSPARQL11 {

    @Override
    protected Query parse$(Query query, String queryString) throws QueryParseException {
        Action action = parser -> parser.QueryUnit();

        if (queryString.toLowerCase().contains("similarity join")){
            SimJoinQuery sj = SimJoinQuery.parse(queryString, action);
            return sj;
        } else {
            query.setSyntax(Syntax.syntaxSPARQL_11);
            perform(query, queryString, action);
            return query;
        }
    }
}

