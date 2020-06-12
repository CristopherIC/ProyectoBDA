import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoCredential;
import com.mongodb.QueryBuilder;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.ValidationOptions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import java.util.logging.Level;
import java.util.logging.Logger;


public class Connection {

	
	static MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://admi:admi@bdainventory-shard-00-00-uuk00.mongodb.net:27017,bdainventory-shard-00-01-uuk00.mongodb.net:27017,bdainventory-shard-00-02-uuk00.mongodb.net:27017/BDAProyecto?ssl=true&replicaSet=BDAInventory-shard-0&authSource=admin&retryWrites=true&w=majority"));
	static MongoDatabase database = mongoClient.getDatabase("BDAProyecto");
	
	static Scanner in = new Scanner(System.in); 	
	
	public static void InsertarCliente() {
		//Conexion a la base de datos necesaria
		MongoCollection <Document> collection = database.getCollection("Cliente");
		
		//Ingresar Datos
		System.out.print("Ingrese el id del Cliente: \n");
		int clienteID = in.nextInt();
		System.out.print("Ingrese el total de reparaciones del cliente: \n");
		int numReparaciones = in.nextInt();
		System.out.print("Ingrese los IDs de las reparaciones \n");
		Integer reparaciones[]= new Integer[numReparaciones];
		for(int i=0; i<numReparaciones; i++ ) {
	         reparaciones[i] = in.nextInt();
	     }
		in.nextLine();
		System.out.print("Ingrese numero de telefono del cliente: \n");
		String telefono = in.nextLine();
		
		System.out.print("Ingrese correo electronico del cliente: \n");
		String correo = in.nextLine();
		
		System.out.print("Ingrese numero de celular del cliente: \n");
		String celular = in.nextLine();
		
		//Crear e insertar documento con la informacion recabada
		Document document = new Document("clienteID", clienteID)
	               			.append("reparacionesID", Arrays.asList(reparaciones))
	               			.append("ordenesTotales", numReparaciones)
	               			.append("contactInfo", new Document("phone", telefono)
	               					.append("email", correo)
	               					.append("cellphone",celular));
	               
		collection.insertOne(document);
		System.out.print("Cliente agregado");
	}
	
	public static void InsertarEmpleado() {
		try { 
		DateFormat format = new SimpleDateFormat("yyyy-dd-MM", Locale.ENGLISH);
		MongoCollection <Document> collection = database.getCollection("Empleado");
		
		System.out.print("Ingrese el id del Empleado: \n");
		int empleadoID = in.nextInt();
		System.out.print("Ingrese el id de la sucursal del empleado: \n");
		int sucursalID = in.nextInt();
		System.out.print("Ingrese el id de el manager del empleado: \n");
		int managerID = in.nextInt();
		
		in.nextLine();
		System.out.print("Ingrese el nombre del empleado: \n");
		String nombre = in.nextLine();
		
		System.out.print("Ingrese el apellido del empleado: \n");
		String apellido = in.nextLine();
		
		System.out.print("Ingrese cumpleaños (YYYY-DD-MM): \n");
		String cumpleaños = in.nextLine();
		
		System.out.print("Ingrese RFC: \n");
		String rfc = in.nextLine();
		
		System.out.print("¿El empleado es manager? (true/false): \n");
		boolean manager = in.nextBoolean();
		
		System.out.print("Ingrese el total de reparaciones realizadas por el empleado: \n");
		int numReparaciones = in.nextInt();
		System.out.print("Ingrese los IDs de las reparaciones \n");
		Integer reparaciones[]= new Integer[numReparaciones];
		for(int i=0; i<numReparaciones; i++ ) {
	         reparaciones[i] = in.nextInt();
	     }
		
		Document document = new Document("employeeID", empleadoID)
       			.append("sucursalID", sucursalID)
       			.append("ManagerID", managerID)
       			.append("Name", nombre)
       			.append("DoB", format.parse(cumpleaños))
       			.append("RFC", rfc)
       			.append("isManager", manager)
       			.append("Reparaciones", Arrays.asList(reparaciones));
       					
       
		collection.insertOne(document);
		System.out.print("Empleado Agregado");
		} catch (ParseException ex) {
            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
        } 
		
	}

	public static void InsertarInventario() {
		try { 
			DateFormat format = new SimpleDateFormat("yyyy-dd-MM", Locale.ENGLISH);
			MongoCollection <Document> collection = database.getCollection("Inventario");
			
			System.out.print("Ingrese el id de la pieza: \n");
			int piezaID = in.nextInt();
			in.nextLine();
			
			System.out.print("Ingrese el nombre de la pieza: \n");
			String nombre = in.nextLine();
			
			System.out.print("Ingrese el id de la sucursal: \n");
			int sucursalID = in.nextInt();
			
			System.out.print("Ingrese la cantidad de piezas: \n");
			int cantidad = in.nextInt();	
			in.nextLine();
			
			System.out.print("Ingrese fecha de la ultima entrega de esta pieza (YYYY-DD-MM): \n");
			String last = in.nextLine();
			
			System.out.print("Ingrese el nombre de quien manofactura esta pieza: \n");
			String manof = in.nextLine();
			
			System.out.print("Ingrese el precio de la pieza: \n");
			int precio = in.nextInt();	
			
			Document document = new Document("piezaID", piezaID)
					.append("piezaDescription", nombre)
	       			.append("sucursalID", sucursalID)
	       			.append("cantidad", cantidad)	       			
	       			.append("lastRefill", format.parse(last))
	       			.append("manufacturer", manof)
	       			.append("price", precio);
	       					
	       
			collection.insertOne(document);
			System.out.print("Inventario Hecho");
			} catch (ParseException ex) {
	            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
	        }
	}
	
	public static void InsertarSucursal() {
		MongoCollection <Document> collection = database.getCollection("Sucursal");
		
		System.out.print("Ingrese el id de la sucursal: \n");
		int sucursalID = in.nextInt();
		System.out.print("Ingrese el id de el manager de la sucursal: \n");
		int managerID = in.nextInt();
		
		in.nextLine();
		
		System.out.print("Ingrese la calle de la sucursal: \n");
		String calle = in.nextLine();
		
		System.out.print("Ingrese el numero en el que se encuentra la sucursal: \n");
		String numero = in.nextLine();
		
		System.out.print("Ingrese el estado en el que esta la sucursal: \n");
		String estado = in.nextLine();
		
		System.out.print("Ingrese numero de telefono de la sucursal: \n");
		String telefono = in.nextLine();
		
		System.out.print("Ingrese correo electronico de la sucursal: \n");
		String correo = in.nextLine();
		
		System.out.print("Ingrese numero de celular de la sucursal: \n");
		String celular = in.nextLine();
		
		Document document = new Document("sucursalID", sucursalID)
       			.append("managerID", managerID)
       			.append("Address", new Document("streetName", calle)
       					.append("number", numero)
       					.append("State", estado))
       			.append("contactInfo", new Document("phone", telefono)
       					.append("email", correo)
       					.append("wapp", celular));
		
		collection.insertOne(document);
		System.out.print("Sucursal Agregada");
		
	}
	
	public static void InsertarReparacion() {
		try { 
			DateFormat format = new SimpleDateFormat("yyyy-dd-MM", Locale.ENGLISH);
			MongoCollection <Document> collection = database.getCollection("Reparación");
			
			System.out.print("Ingrese el id de la orden: \n");
			int ordenID = in.nextInt();
			System.out.print("Ingrese el id de el cliente: \n");
			int clienteID = in.nextInt();		
			in.nextLine();
			System.out.print("Ingrese descripcion de la reparacion: \n");
			String desc = in.nextLine();
			System.out.print("Ingrese fecha de inicio de la reparación (YYYY-DD-MM): \n");
			String ini = in.nextLine();
			System.out.print("Ingrese fecha de termino de la reparación (YYYY-DD-MM): \n");
			String fin = in.nextLine();
			
			System.out.print("Ingrese id de la sucursal: \n");
			int sucursalID = in.nextInt();	
			in.nextLine();	
			
			System.out.print("Ingrese comentarios extra acerca de la reparación: \n");
			String comentarios = in.nextLine();
			
			System.out.print("Ingrese el costo de la reparación: \n");
			int costo = in.nextInt();	
			
			System.out.print("Ingrese el total de piezas usadas: \n");
			int numReparaciones = in.nextInt();
			System.out.print("Ingrese los IDs de las piezas utilizadas \n");
			Integer piezas[]= new Integer[numReparaciones];
			for(int i=0; i<numReparaciones; i++ ) {
		         piezas[i] = in.nextInt();
		     }
			
			Document document = new Document("ordenID", ordenID)
					.append("clienteID", clienteID)
	       			.append("OrdenDesc", desc)
	       			.append("fechaEntrega", format.parse(ini))	       			
	       			.append("fechaSalida", format.parse(fin))	
	       			.append("sucursalID", sucursalID)
	       			.append("comentarios", comentarios)
	       			.append("costo", costo)
	       			.append("PiezasID", Arrays.asList(piezas));
	       					
	       
			collection.insertOne(document);
			System.out.print("Inventario Hecho \n");
			ActualizaCliente(clienteID, ordenID);
			} catch (ParseException ex) {
	            Logger.getLogger(Connection.class.getName()).log(Level.SEVERE, null, ex);
	        }		
	}
	
	//Cada orden que se cree se agregara a los datos del cliente correspondiente
	public static void ActualizaCliente(int clienteID, int ordenID) {
		MongoCollection <Document> collection = database.getCollection("Cliente");
		//Identifica el documento del cliente 
		Document found = (Document) collection.find(new Document("clienteID",clienteID)).first();
		
		//Actualizamos valores
		collection.updateMany(found,Updates.addToSet("reparacionesID", ordenID));
		
		//Obtenemos el total de ordenes para agregar 1 mas
		Integer total = found.getInteger("ordenesTotales");	
					
		
		collection.updateMany(found,Updates.set("ordenesTotales", total+1));		
		System.out.println("Información actualizada");
	}
	
	public static void Query(int precio) {
		MongoCollection <Document> collection = database.getCollection("Inventario");
		
		FindIterable<Document> findIt = collection.find( new Document()
				.append("price", new Document()
				.append("$gt" , precio)
				));

				MongoCursor<Document> cursor = findIt.iterator();
				try {
				while(cursor.hasNext()) {
				System.out.println(cursor.next());
				}
				} finally {
				cursor.close();
				}
	}
	
	public static void ConsultaCliente() {
		MongoCollection <Document> collection = database.getCollection("Cliente");
		System.out.print("Ingrese el id del cliente: \n");
		int clienteID = in.nextInt();
		Document found = (Document) collection.find(new Document("clienteID",clienteID)).first();
		System.out.println(found);
	}
	
	
	public static void main(String[] args) throws InterruptedException {
		//Elimina los logs de mongodb en consola, pues estorbaba para el correcto funcionamiento
		Logger mongoLogger = Logger.getLogger( "org.mongodb.driver" );
		mongoLogger.setLevel(Level.SEVERE);
		
		//InsertarCliente();
		//ActualizaCliente(617, 1111);
		//InsertarReparacion();
		//ConsultaCliente();
		//Query(450);
	}

}
