import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.tdb.TDB;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.mgt.TDBMgt;
import org.apache.jena.tdb.setup.DatasetBuilderStd;
import org.apache.jena.tdb.setup.StoreParams;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.tdb.store.GraphNonTxnTDB;
import org.apache.jena.tdb.sys.TDBInternal;
import org.apache.jena.tdb.transaction.DatasetBuilderTxn;
import org.apache.jena.util.FileManager;
import org.apache.jena.web.DatasetGraphAccessor;

import java.io.IOException;

public class Benchmark {

    static public void main(String[] args) throws IOException {

        Dataset wikidata = TDBFactory.createDataset(args[0]);

        String q = "select distinct ?p where {?o ?p ?q}";
        Query query = QueryFactory.create(q);
        QueryIterator res = Algebra.exec(Algebra.compile(query), wikidata.getDefaultModel());
        for ( ; res.hasNext() ; )
        {
            Binding b = res.nextBinding() ;
            System.out.println(b.toString()) ;
        }
        res.close();
    }

    static private void loadData(String filename){
        Dataset wikidata = TDBFactory.createDataset("C:\\Users\\scfer\\Documents\\Universidad\\Doctorado\\Jena\\dataset");
        FileManager.get().readModel(wikidata.getDefaultModel(), filename);
        wikidata.close();
        System.exit(0);
    }

}
