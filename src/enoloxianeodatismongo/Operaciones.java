package enoloxianeodatismongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.neodatis.odb.ODB;
import org.neodatis.odb.ODBFactory;
import org.neodatis.odb.Objects;
import org.neodatis.odb.core.query.IQuery;
import org.neodatis.odb.core.query.criteria.Where;
import org.neodatis.odb.impl.core.query.criteria.CriteriaQuery;

public class Operaciones {

    MongoClient cliente;
    MongoDatabase base;
    MongoCollection<Document> colecion;

    ODB odb;
    int acidez;
    String trataAcidez;
    int total;
    String dni;
    String codigo;
    String nomeUva;

    public void amosarNeodatis() {

        Objects<Analisis> analisis = odb.getObjects(Analisis.class);

        // display each object
        Analisis anl = null;
        while (analisis.hasNext()) {
            anl = analisis.next();
            System.out.println(anl.toString());

            //recogemos el tipo de uva del analisis
            String tipoUva = anl.getTipouva();
            acidez = anl.getAcidez();
            //recogemos dni para comparar con la tabla clientes e incrementar en 1 el numero de analisis de los clientes
            dni = anl.getDni();
            //recogemos la cantidade y la multiplicamos por 15
            total = anl.getCantidade() * 15;
            codigo = anl.getCodigoa();

          //query por tipoUva,donde comparamos las dos tablas,Analisis y uva
            IQuery queryUvas = new CriteriaQuery(Uva.class, Where.equal("tipouva", tipoUva));
            //Recogemos los objetos previstos en la query
            Objects<Uva> uvas = odb.getObjects(queryUvas);
            //inicializamos un objeto a null para poder iterar por el y recoger sus variables
            Uva uva = null;
            while (uvas.hasNext()) {

                uva = uvas.next();
                int min = uva.getAcidezmin();
                int max = uva.getAcidezmax();
                nomeUva = uva.getNomeu();
                if (acidez < min) {
                    trataAcidez = "Subir acidez";
                } else if (acidez > max) {
                    trataAcidez = "Baixar acidez";
                } else {
                    trataAcidez = "Acidez correcta";
                }

            }

            //Query por DNI,el cual recogemos previamente de analisis para comprar y solo aumentar en 1 la tabla Cliente
            IQuery queryClientes = new CriteriaQuery(Cliente.class, Where.equal("dni", dni));
            Objects<Cliente> clientes = odb.getObjects(queryClientes);

            Cliente cliente = null;

            while (clientes.hasNext()) {
                cliente = clientes.next();
                System.out.println(cliente.toString());
                //cliente.setNumerodeanalisis(cliente.getNumerodeanalisis()+1);
                //odb.store(cliente);

            }
            //Con metodo insertMongo(codigo,nomeUva,trataAcidez,total);

            /*Sin metodo
             cliente = new MongoClient("localhost",27017);
             base = cliente.getDatabase("resultado");
             colecion = base.getCollection("xerado");
             Document d = new Document("_id",codigo)
             .append("uva", nomeUva)
             .append("tratacidez", trataAcidez)
             .append("total", total);
             colecion.insertOne(d);
            
            
             */
        }

    }

    public void openBase() {
        odb = ODBFactory.open("vinho");

    }

    public void closeBase() {
        odb.close();

    }

    public void insertMongo(String codigo, String nomeUva, String trataAcidez, int total) {

        cliente = new MongoClient("localhost", 27017);
        base = cliente.getDatabase("resultado");
        colecion = base.getCollection("xerado");
        Document d = new Document("_id", codigo)
                .append("uva", nomeUva)
                .append("tratacidez", trataAcidez)
                .append("total", total);
        colecion.insertOne(d);

    }

}
