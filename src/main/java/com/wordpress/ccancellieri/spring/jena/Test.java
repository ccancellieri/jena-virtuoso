package com.wordpress.ccancellieri.spring.jena;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtModel;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.InfModel;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.util.NodeUtils;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.hp.hpl.jena.vocabulary.ResultSet;

public class Test {
    
//    Edit the sample programs VirtuosoSPARQLExampleX.java, where X = 1 to 9. Set the JDBC connection strings therein to point to a valid Virtuoso Server instance, using the form:
//
//
//        "jdbc:virtuoso://<virtuoso-hostname-or-IP-address>[:<data port>]/charset=UTF-8/log_enable=2", "<username>", "<password>"   
//
//
//        For example,
//
//
//        "jdbc:virtuoso://localhost:1111/charset=UTF-8/log_enable=2", "dba", "dba"   


    /**
     * Executes a SPARQL query against a virtuoso url and prints results.
     */
    public static void main13(String[] args) 
    {
        String url;
        if(args.length == 0)
            url = "jdbc:virtuoso://localhost:1111";
        else
            url = args[0];

/*** LOADING data to http://exmpl13 graph  ***/
        
        Model mdata = VirtModel.createDatabaseModel("http://exmpl13", url, "dba", "dba");
        mdata.removeAll();

        Statement st;

        st = statement(mdata, "http://localhost:8890/dataspace http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://rdfs.org/sioc/ns#Space" );
        mdata.add(st);

        st = statement(mdata, "http://localhost:8890/dataspace http://rdfs.org/sioc/ns#link http://localhost:8890/ods");
        mdata.add(st);


        st = statement(mdata, "http://localhost:8890/dataspace/test2/weblog/test2tWeblog http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://rdfs.org/sioc/types#Weblog");
        mdata.add(st);
        st = statement(mdata, "http://localhost:8890/dataspace/test2/weblog/test2tWeblog http://rdfs.org/sioc/ns#link http://localhost:8890/dataspace/test2/weblog/test2tWeblog");
        mdata.add(st);

        st = statement(mdata, "http://localhost:8890/dataspace/discussion/oWiki-test1Wiki http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://rdfs.org/sioc/types#MessageBoard");
        mdata.add(st);
        st = statement(mdata, "http://localhost:8890/dataspace/discussion/oWiki-test1Wiki http://rdfs.org/sioc/ns#link http://localhost:8890/dataspace/discussion/oWiki-test1Wiki");
        mdata.add(st);


        // Query string.
        String queryString = "SELECT * WHERE {?s ?p ?o}" ; 
        System.out.println("Execute query=\n"+queryString) ;
        System.out.println() ;


        QueryExecution qexec = VirtuosoQueryExecutionFactory.create(queryString, mdata) ;
        try {
            com.hp.hpl.jena.query.ResultSet rs = qexec.execSelect() ;
            for ( ; rs.hasNext() ; ) {
                QuerySolution result = rs.nextSolution();
                    RDFNode s = result.get("s");
                    RDFNode p = result.get("p");
                    RDFNode o = result.get("o");
                    System.out.println(" { " + s + " " + p + " " + o + " . }");
            }
        } finally {
            qexec.close() ;
        }

        mdata.removeRuleSet("exmpl13_rules","http://:exmpl13_schema");


/*** LOADING rule to http://exmpl13_schema graph  ***/

        VirtModel mrule = VirtModel.openDatabaseModel("http://exmpl13_schema", url, "dba", "dba");
        mrule.removeAll();

        Resource r1 = mrule.createResource("http://rdfs.org/sioc/ns#Space") ;
        r1.addProperty(RDFS.subClassOf, rdfNode(mrule, "http://www.w3.org/2000/01/rdf-schema#Resource"));

        r1 = mrule.createResource("http://rdfs.org/sioc/ns#Container") ;
        r1.addProperty(RDFS.subClassOf, rdfNode(mrule, "http://rdfs.org/sioc/ns#Space"));

        r1 = mrule.createResource("http://rdfs.org/sioc/ns#Forum") ;
        r1.addProperty(RDFS.subClassOf, rdfNode(mrule, "http://rdfs.org/sioc/ns#Container"));

        r1 = mrule.createResource("http://rdfs.org/sioc/types#Weblog") ;
        r1.addProperty(RDFS.subClassOf, rdfNode(mrule, "http://rdfs.org/sioc/ns#Forum"));

        r1 = mrule.createResource("http://rdfs.org/sioc/types#MessageBoard") ;
        r1.addProperty(RDFS.subClassOf, rdfNode(mrule, "http://rdfs.org/sioc/ns#Forum"));

        r1 = mrule.createResource("http://rdfs.org/sioc/ns#link") ;
        r1.addProperty(RDFS.subPropertyOf, rdfNode(mrule, "http://rdfs.org/sioc/ns"));

        mrule.close();

        mdata.createRuleSet("exmpl13_rules","http://exmpl13_schema");
        mdata.close();



        VirtInfGraph infGraph = new VirtInfGraph("exmpl13_rules", false, 
                                        "http://exmpl13", url, "dba", "dba");
        InfModel model = ModelFactory.createInfModel(infGraph);
        
        
        queryString = "SELECT ?s "+
                      "FROM <http://exmpl13> "+
                      "WHERE {?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://rdfs.org/sioc/ns#Space> } ";
        System.out.println("\n\nExecute query=\n"+queryString) ;
        System.out.println() ;

        qexec = VirtuosoQueryExecutionFactory.create(queryString, model) ;
        try {
            com.hp.hpl.jena.query.ResultSet rs = qexec.execSelect() ;
            for ( ; rs.hasNext() ; ) {
                QuerySolution result = rs.nextSolution();
                    RDFNode s = result.get("s");
                    System.out.println(" " + s);
            }
        } finally {
            qexec.close() ;
        }



        queryString = "SELECT * "+
                      "FROM <http://exmpl13> "+
                      "WHERE "+
                      "{ "+
                      " ?s ?p <http://rdfs.org/sioc/ns#Space> . "+
                      " ?s ?p1 <http://localhost:8890/dataspace/test2/weblog/test2tWeblog> . "+
                      "} ";
        
        System.out.println("\n\nExecute query=\n"+queryString) ;
        System.out.println() ;

        qexec = VirtuosoQueryExecutionFactory.create(queryString, model) ;
        try {
            com.hp.hpl.jena.query.ResultSet rs = qexec.execSelect() ;
            for ( ; rs.hasNext() ; ) {
                QuerySolution result = rs.nextSolution();
                    RDFNode s = result.get("s");
                    RDFNode p = result.get("p");
                    RDFNode p1 = result.get("p1");
                    System.out.println(" " + s + " " + p + " " + p1);
            }
        } finally {
            qexec.close() ;
        }

        model.close();
    }



    public static Statement statement( Model m, String fact )
         {
         StringTokenizer st = new StringTokenizer( fact );
         Resource sub = resource( m, st.nextToken() );
         Property pred = property( m, st.nextToken() );
         RDFNode obj = rdfNode( m, st.nextToken() );
         return m.createStatement( sub, pred, obj );    
         }    

    public static Resource resource( Model m, String s )
        { return (Resource) rdfNode( m, s ); }

    public static Property property( Model m, String s )
        { return (Property) rdfNode( m, s ).as( Property.class ); }

    public static RDFNode rdfNode( Model m, String s )
        { return m.asRDFNode( NodeUtils.create( m, s ) ); }
    
    /**
     * Executes a SPARQL query against a virtuoso url and prints results.
     */
    public static void main9(String[] args) {

            String url;
            if(args.length == 0)
                url = "jdbc:virtuoso://localhost:1111";
            else
                url = args[0];

/*                      STEP 1                  */
            VirtGraph set = new VirtGraph (url, "dba", "dba");

/*                      STEP 2                  */
            String str = "CLEAR GRAPH <http://test1>";
            VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(str, set);
            vur.exec();                  

            str = "INSERT INTO GRAPH <http://test1> { <http://aa> <http://bb> 'cc' . <http://aa1> <http://bb> 123. }";
            vur = VirtuosoUpdateFactory.create(str, set);
            vur.exec();                  


/*              Select all data in virtuoso     */
            com.hp.hpl.jena.query.Query sparql = QueryFactory.create("SELECT * FROM <http://test1> WHERE { ?s ?p ?o }");
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (sparql, set);
            com.hp.hpl.jena.query.ResultSet results = vqe.execSelect();
            System.out.println("\nSELECT results:");
            while (results.hasNext()) {
                    QuerySolution rs = results.nextSolution();
                RDFNode s = rs.get("s");
                RDFNode p = rs.get("p");
                RDFNode o = rs.get("o");
                System.out.println(" { " + s + " " + p + " " + o + " . }");
            }

            sparql = QueryFactory.create("DESCRIBE <http://aa> FROM <http://test1>");
            vqe = VirtuosoQueryExecutionFactory.create (sparql, set);

            Model model = vqe.execDescribe();
            Graph g = model.getGraph();
            System.out.println("\nDESCRIBE results:");
            for (Iterator i = g.find(Node.ANY, Node.ANY, Node.ANY); i.hasNext();) 
               {
                  Triple t = (Triple)i.next();
                  System.out.println(" { " + t.getSubject() + " " + 
                                             t.getPredicate() + " " + 
                                             t.getObject() + " . }");
            }



            sparql = QueryFactory.create("CONSTRUCT { ?x <http://test> ?y } FROM <http://test1> WHERE { ?x <http://bb> ?y }");
            vqe = VirtuosoQueryExecutionFactory.create (sparql, set);

            model = vqe.execConstruct();
            g = model.getGraph();
            System.out.println("\nCONSTRUCT results:");
            for (Iterator i = g.find(Node.ANY, Node.ANY, Node.ANY); i.hasNext();) 
               {
                  Triple t = (Triple)i.next();
                  System.out.println(" { " + t.getSubject() + " " + 
                                             t.getPredicate() + " " + 
                                             t.getObject() + " . }");
            }


            sparql = QueryFactory.create("ASK FROM <http://test1> WHERE { <http://aa> <http://bb> ?y }");
            vqe = VirtuosoQueryExecutionFactory.create (sparql, set);

            boolean res = vqe.execAsk();
            System.out.println("\nASK results: "+res);


    }
    
    /**
     * Executes a SPARQL query against a virtuoso url and prints results.
     */
    public static void main8(String[] args) {

        String url;
        if (args.length == 0)
            url = "jdbc:virtuoso://localhost:1111";
        else
            url = args[0];

        /* STEP 1 */
        VirtGraph set = new VirtGraph(url, "dba", "dba");

        /* STEP 2 */
        System.out.println("\nexecute: CLEAR GRAPH <http://test1>");
        String str = "CLEAR GRAPH <http://test1>";
        VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(str, set);
        vur.exec();

        System.out
            .println("\nexecute: INSERT INTO GRAPH <http://test1> { <aa> <bb> 'cc' . <aa1> <bb1> 123. }");
        str = "INSERT INTO GRAPH <http://test1> { <aa> <bb> 'cc' . <aa1> <bb1> 123. }";
        vur = VirtuosoUpdateFactory.create(str, set);
        vur.exec();

        /* STEP 3 */
        /* Select all data in virtuoso */
        System.out.println("\nexecute: SELECT * FROM <http://test1> WHERE { ?s ?p ?o }");
        com.hp.hpl.jena.query.Query sparql = QueryFactory.create("SELECT * FROM <http://test1> WHERE { ?s ?p ?o }");

        /* STEP 4 */
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, set);

        com.hp.hpl.jena.query.ResultSet results = vqe.execSelect();
        while (results.hasNext()) {
            QuerySolution rs = results.nextSolution();
            RDFNode s = rs.get("s");
            RDFNode p = rs.get("p");
            RDFNode o = rs.get("o");
            System.out.println(" { " + s + " " + p + " " + o + " . }");
        }

        System.out.println("\nexecute: DELETE FROM GRAPH <http://test1> { <aa> <bb> 'cc' }");
        str = "DELETE FROM GRAPH <http://test1> { <aa> <bb> 'cc' }";
        vur = VirtuosoUpdateFactory.create(str, set);
        vur.exec();

        System.out.println("\nexecute: SELECT * FROM <http://test1> WHERE { ?s ?p ?o }");
        vqe = VirtuosoQueryExecutionFactory.create(sparql, set);
        results = vqe.execSelect();
        while (results.hasNext()) {
            QuerySolution rs = results.nextSolution();
            RDFNode s = rs.get("s");
            RDFNode p = rs.get("p");
            RDFNode o = rs.get("o");
            System.out.println(" { " + s + " " + p + " " + o + " . }");
        }
    }
    
    public static void main7(String[] args)
    {
        String url;
        if(args.length == 0)
            url = "jdbc:virtuoso://localhost:1111";
        else
            url = args[0];

        Node foo1 = Node.createURI("http://example.org/#foo1");
        Node bar1 = Node.createURI("http://example.org/#bar1");
        Node baz1 = Node.createURI("http://example.org/#baz1");

        Node foo2 = Node.createURI("http://example.org/#foo2");
        Node bar2 = Node.createURI("http://example.org/#bar2");
        Node baz2 = Node.createURI("http://example.org/#baz2");

        Node foo3 = Node.createURI("http://example.org/#foo3");
        Node bar3 = Node.createURI("http://example.org/#bar3");
        Node baz3 = Node.createURI("http://example.org/#baz3");

        List triples1 = new ArrayList();
        triples1.add(new Triple(foo1, bar1, baz1));
        triples1.add(new Triple(foo2, bar2, baz2));
        triples1.add(new Triple(foo3, bar3, baz3));

        List triples2 = new ArrayList();
        triples2.add(new Triple(foo1, bar1, baz1));
        triples2.add(new Triple(foo2, bar2, baz2));

        VirtGraph graph = new VirtGraph ("Example7", url, "dba", "dba");

        graph.clear ();

        System.out.println("graph.isEmpty() = " + graph.isEmpty());
        System.out.println("Add List with 3 triples to graph <Example7> via BulkUpdateHandler.");

        graph.getBulkUpdateHandler().add(triples1);

        System.out.println("graph.isEmpty() = " + graph.isEmpty());
        System.out.println("graph.getCount() = " + graph.getCount());

        ExtendedIterator iter = graph.find(Node.ANY, Node.ANY, Node.ANY);
        System.out.println ("\ngraph.find(Node.ANY, Node.ANY, Node.ANY) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());


        System.out.println("\n\nDelete List of 2 triples from graph <Example7> via BulkUpdateHandler.");

        graph.getBulkUpdateHandler().delete(triples2);

        System.out.println("graph.isEmpty() = " + graph.isEmpty());
        System.out.println("graph.getCount() = " + graph.getCount());

        iter = graph.find(Node.ANY, Node.ANY, Node.ANY);
        System.out.println ("\ngraph.find(Node.ANY, Node.ANY, Node.ANY) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());

        graph.clear ();
        System.out.println("\nCLEAR graph <Example7>");

    }
    
    public static void main6(String[] args)
    {
        String url;
        if(args.length == 0)
            url = "jdbc:virtuoso://localhost:1111";
        else
            url = args[0];

        Node foo1 = Node.createURI("http://example.org/#foo1");
        Node bar1 = Node.createURI("http://example.org/#bar1");
        Node baz1 = Node.createURI("http://example.org/#baz1");

        Node foo2 = Node.createURI("http://example.org/#foo2");
        Node bar2 = Node.createURI("http://example.org/#bar2");
        Node baz2 = Node.createURI("http://example.org/#baz2");

        Node foo3 = Node.createURI("http://example.org/#foo3");
        Node bar3 = Node.createURI("http://example.org/#bar3");
        Node baz3 = Node.createURI("http://example.org/#baz3");

        VirtGraph graph = new VirtGraph ("Example6", url, "dba", "dba");

        graph.clear ();

        System.out.println("graph.isEmpty() = " + graph.isEmpty());

        System.out.println("test Transaction Commit.");
        graph.getTransactionHandler().begin();
        System.out.println("begin Transaction.");
        System.out.println("Add 3 triples to graph <Example6>.");

        graph.add(new Triple(foo1, bar1, baz1));
        graph.add(new Triple(foo2, bar2, baz2));
        graph.add(new Triple(foo3, bar3, baz3));

        graph.getTransactionHandler().commit();
        System.out.println("commit Transaction.");
        System.out.println("graph.isEmpty() = " + graph.isEmpty());
        System.out.println("graph.getCount() = " + graph.getCount());

        ExtendedIterator iter = graph.find(Node.ANY, Node.ANY, Node.ANY);
        System.out.println ("\ngraph.find(Node.ANY, Node.ANY, Node.ANY) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());

        graph.clear ();
        System.out.println("\nCLEAR graph <Example6>");
        System.out.println("graph.isEmpty() = " + graph.isEmpty());

        System.out.println("Add 1 triples to graph <Example6>.");
        graph.add(new Triple(foo1, bar1, baz1));

        System.out.println("test Transaction Abort.");
        graph.getTransactionHandler().begin();
        System.out.println("begin Transaction.");
        System.out.println("Add 2 triples to graph <Example6>.");

        graph.add(new Triple(foo2, bar2, baz2));
        graph.add(new Triple(foo3, bar3, baz3));

        graph.getTransactionHandler().abort();
        System.out.println("abort Transaction.");
        System.out.println("graph.isEmpty() = " + graph.isEmpty());
        System.out.println("graph.getCount() = " + graph.getCount());

        iter = graph.find(Node.ANY, Node.ANY, Node.ANY);
        System.out.println ("\ngraph.find(Node.ANY, Node.ANY, Node.ANY) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());

        graph.clear ();
        System.out.println("\nCLEAR graph <Example6>");
    }
    
    public static void main5(String[] args)
    {
        String url;
        if(args.length == 0)
            url = "jdbc:virtuoso://localhost:1111";
        else
            url = args[0];

        Node foo1 = Node.createURI("http://example.org/#foo1");
        Node bar1 = Node.createURI("http://example.org/#bar1");
        Node baz1 = Node.createURI("http://example.org/#baz1");

        Node foo2 = Node.createURI("http://example.org/#foo2");
        Node bar2 = Node.createURI("http://example.org/#bar2");
        Node baz2 = Node.createURI("http://example.org/#baz2");

        Node foo3 = Node.createURI("http://example.org/#foo3");
        Node bar3 = Node.createURI("http://example.org/#bar3");
        Node baz3 = Node.createURI("http://example.org/#baz3");

        VirtGraph graph = new VirtGraph ("Example5", url, "dba", "dba");

        graph.clear ();

        System.out.println("graph.isEmpty() = " + graph.isEmpty());

        System.out.println("Add 3 triples to graph <Example5>.");

        graph.add(new Triple(foo1, bar1, baz1));
        graph.add(new Triple(foo2, bar2, baz2));
        graph.add(new Triple(foo3, bar3, baz3));
        graph.add(new Triple(foo1, bar2, baz2));
        graph.add(new Triple(foo1, bar3, baz3));

        System.out.println("graph.isEmpty() = " + graph.isEmpty());
        System.out.println("graph.getCount() = " + graph.getCount());

        ExtendedIterator iter = graph.find(foo1, Node.ANY, Node.ANY);
        System.out.println ("\ngraph.find(foo1, Node.ANY, Node.ANY) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());

        iter = graph.find(Node.ANY, Node.ANY, baz3);
        System.out.println ("\ngraph.find(Node.ANY, Node.ANY, baz3) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());

        iter = graph.find(foo1, Node.ANY, baz3);
        System.out.println ("\ngraph.find(foo1, Node.ANY, baz3) \nResult:");
        for ( ; iter.hasNext() ; )
            System.out.println ((Triple) iter.next());

        graph.clear ();

    }
}
