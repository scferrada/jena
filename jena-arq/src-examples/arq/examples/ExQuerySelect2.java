/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arq.examples;


// The ARQ application API.
import org.apache.jena.assembler.Mode;
import org.apache.jena.atlas.io.IndentedWriter ;
import org.apache.jena.query.* ;
import org.apache.jena.rdf.model.Model ;
import org.apache.jena.rdf.model.ModelFactory ;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource ;
import org.apache.jena.vocabulary.DC ;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/** Example 2 : Execute a simple SELECT query on a model
 *  to find the DC titles contained in a model. 
 *  Show how to print results twice. */

public class ExQuerySelect2
{
    static public final String NL = System.getProperty("line.separator") ; 
    
    public static void main(String[] args)
    {
        // Create the data.
        // This wil be the background (unnamed) graph in the dataset.
        Model model = createModel() ;
        
        // First part or the query string 
        String prolog = "PREFIX dc: <"+DC.getURI()+">" ;
        
        // Query string.
        String queryString = prolog + NL +
            "SELECT ?title WHERE {?x dc:title ?title}" ; 
        
        Query query = QueryFactory.create(queryString) ;
        // Print with line numbers
        query.serialize(new IndentedWriter(System.out,true)) ;
        System.out.println() ;
        
        // Create a single execution of this query, apply to a model
        // which is wrapped up as a Dataset
        
        // Or QueryExecutionFactory.create(queryString, model) ;        
        try(QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            // A ResultSet is an iterator - any query solutions returned by .next()
            // are not accessible again.
            // Create a ResultSetRewindable that can be reset to the beginning.
            // Do before first use.
            
            ResultSetRewindable rewindable = ResultSetFactory.makeRewindable(qexec.execSelect()) ;
            ResultSetFormatter.out(rewindable) ;
            rewindable.reset() ;
            ResultSetFormatter.out(rewindable) ;
        }
    }
    
    public static Model createModel()
    {
        Model m = ModelFactory.createDefaultModel() ;
        
        Resource r1 = m.createResource("http://example.org/book#1") ;
        Resource r2 = m.createResource("http://example.org/book#2") ;
        
        r1.addProperty(DC.title, "SPARQL - the book")
          .addProperty(DC.description, "A book about SPARQL") ;
        
        r2.addProperty(DC.title, "Advanced techniques for SPARQL") ;
        
        return m ;
    }

    public static Model createSimModel(){
        Model m = ModelFactory.createDefaultModel();
        Property a = m.createProperty("http://ex.com/a");
        Property b = m.createProperty("http://ex.com/b");
        int N = 1000;
        for (int i = 0; i < N; i++) {
            Resource r = m.createResource("http://ex.com/"+i);
            r.addProperty(a, ""+i).addProperty(b, ""+i);
        }
        return m;
    }

    public static Model createSimModel2(String path){
        Model m = ModelFactory.createDefaultModel();
        List<Property> properties = new LinkedList<>();
        Property a = m.createProperty("http://ex.com/a"); properties.add(a);
        Property b = m.createProperty("http://ex.com/b"); properties.add(b);
        Property c = m.createProperty("http://ex.com/c"); properties.add(c);
        Property d = m.createProperty("http://ex.com/d"); properties.add(d);
        Property e = m.createProperty("http://ex.com/e"); properties.add(e);
        Property f = m.createProperty("http://ex.com/f"); properties.add(f);
        Property g = m.createProperty("http://ex.com/g"); properties.add(g);
        Property h = m.createProperty("http://ex.com/h"); properties.add(h);
        Property i = m.createProperty("http://ex.com/i"); properties.add(i);
        Property j = m.createProperty("http://ex.com/j"); properties.add(j);

        try (Stream<String> lines = Files.lines(Paths.get(path))) {
            lines.forEachOrdered(line->{
                List<String> parts = Arrays.asList(line.split(" "));
                Resource r = m.createResource("http://ex.com/"+parts.get(0));
                for (int k = 0; k < properties.size(); k++) {
                    r.addProperty(properties.get(k), parts.get(k));
                }
            });
        } catch (IOException ioe) {
            return null;
        }
        return m;
    }
}
