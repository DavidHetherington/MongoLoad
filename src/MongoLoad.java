/**
 * 
 */
import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author David Hetherington
 * 
 * 		    Quick-and-Dirty program to load the "Cities" collection in the
 * 			my-city-guide database on MongoLab.com. The first four cities
 * 			were loaded manually using the administrator port on Mongolab.com.
 * 			The rest are loaded by this program from a CSV file. 
 * 
 * 			Note: that there is a remaining I/O exception after the last 
 * 			city. However, we only needed this program to run once and
 * 			it did load all the city data successfully. Not worth debugging
 * 			the lingering exception.
 * 
 * 			David Hetherington 11 December 2015
 *
 */
public class MongoLoad {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String csvFile = "C:/Temp/Cities-All-But-First_Four.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		
		System.out.println("Welcome to MongoLoad.");

		//  Edit the following line to add your unique Mongolab credentials
		//  Freduser should be replaced with the username of the database user for your database
		//  Secret1234 should be replaced with the database user password. Note: special characters are
		//             problematic - stick with numbers and letters
		//  ds012345 should be replaced by the ID number provided by Mongolab when you setup your database
		//  my-city-guide is the name of your database. Either name it exactly that ("my-city-guide") when you set up
		//             at MongoLab.com or replace this string in two locations below with your actual database name.
		//  Cities is the name of the collection in withing my-city-guide
		MongoClientURI connectionString = new MongoClientURI("mongodb://Freduser:Secret1234@ds012345.mongolab.com:27835/my-city-guide");
		System.out.println("Connection String is OK.");
		
		MongoClient mongoClient = new MongoClient(connectionString);

		System.out.println("Client is OK."); 
		
		MongoDatabase database = mongoClient.getDatabase("my-city-guide");
		
		System.out.println("database is OK. "+database.getName());
		
		MongoCollection<Document> collection = database.getCollection("Cities");
		
		System.out.println("collection is OK. ");
		
		System.out.println("So far we have "+collection.count()+" cities in the database.");

		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

			        // use comma as separator
				String[] city = line.split(cvsSplitBy);
				
	
				System.out.println("City =  "        + city[0] 
	                                 + " , State = " + city[1]
	                                 + " , Population = " + city[2]
	                                 + " , Crime = " + city[3]
	                                 + " , Cost of Living = " + city[4]);
				
				Document doc = new Document("City", city[0])
			               .append("State", city[1])
			               .append("Population", Integer.parseInt(city[2]))
			               .append("Violent Crime", Float.parseFloat(city[3]))
			               .append("Cost of Living", Float.parseFloat(city[4]));
				
				collection.insertOne(doc);

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("Done");
		
	}

}
