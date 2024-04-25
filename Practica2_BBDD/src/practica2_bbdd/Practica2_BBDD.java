package practica2_bbdd;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
/**
 * @author arvil
 */
public class Practica2_BBDD {
    /**
     * @param args the command line arguments
     */
        
        static String url = "jdbc:mysql://localhost:3306/";
        static String user = "root";
        static String passwd = "";
        static String driver = "com.mysql.cj.jdbc.Driver";
        static Connection cx;
        
        public static Connection conectar(String bd){
            
            try{
                Class.forName(driver);
                cx = DriverManager.getConnection(url+bd, user, passwd);
                System.out.println("Se ha conectado a la Base de Datos " + bd);
            }catch(ClassNotFoundException |SQLException ex){
                System.out.println("No se pudo conectar a la Base de Datos " + bd);
            }
            return cx;
        }
        
        public static void desconectar(Connection cn, String bd){
            try{
                cn.close();
                System.out.println("Se ha desconectado de la Base de Datos " + bd);
            }catch(SQLException e){
                System.out.println("No se pudo desconectar a la Base de Datos ");
            }
        }
        
        public static void consulta1(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select * from actor";
            Statement stmt;
            ResultSet rs;
            
            int id;
            String nombre;
            String apellido;
            String fecha;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    id = rs.getInt("actor_id");
                    nombre = rs.getString("first_name");
                    apellido = rs.getString("last_name");
                    fecha = rs.getString("last_update");
                    
                    System.out.println(id + " | " + nombre + " | " + apellido + " | " + fecha);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta2(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select * from actor order by last_name limit 15";
            Statement stmt;
            ResultSet rs;
            
            int id;
            String nombre;
            String apellido;
            String fecha;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    id = rs.getInt("actor_id");
                    nombre = rs.getString("first_name");
                    apellido = rs.getString("last_name");
                    fecha = rs.getString("last_update");
                    
                    System.out.println(id + " | " + nombre + " | " + apellido + " | " + fecha);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta3(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select * from city";
            Statement stmt;
            ResultSet rs;
            
            int id;
            String nombre;
            String pais;
            String distrito;
            int poblacion;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    id = rs.getInt("ID");
                    nombre = rs.getString("Name");
                    pais = rs.getString("CountryCode");
                    distrito = rs.getString("District");
                    poblacion = rs.getInt("Population");
                    
                    System.out.println(id + " | " + nombre + " | " + pais + " | " + distrito + " | " + poblacion);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta4(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select * from city where CountryCode = 'ESP'";
            Statement stmt;
            ResultSet rs;
            
            int id;
            String nombre;
            String pais;
            String distrito;
            int poblacion;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    id = rs.getInt("ID");
                    nombre = rs.getString("Name");
                    pais = rs.getString("CountryCode");
                    distrito = rs.getString("District");
                    poblacion = rs.getInt("Population");
                    
                    System.out.println(id + " " + nombre + " | " + pais + " " + distrito + " | " + poblacion);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta5(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select customer_id, sum(amount) as dinero_total from payment group by customer_id";
            Statement stmt;
            ResultSet rs;
            
            int id_cliente;
            double totalGastado;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    id_cliente = rs.getInt("customer_id");
                    totalGastado = rs.getInt("dinero_total");
                    
                    System.out.println("id: " + id_cliente + " | " + "Dinero total: " + totalGastado );
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta6(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select rating, avg(length) as duracion_media from film group by rating";
            Statement stmt;
            ResultSet rs;
            
            String rating;
            double duracionMedia;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    rating = rs.getString("rating");
                    duracionMedia = rs.getDouble("duracion_media");
                    
                    System.out.println(rating + " | " + duracionMedia + " minutos");
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta7(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select country.code as country_code, count(City.ID) as city_count from country left join City on country.code =City.CountryCode Group by Country.Code";
            Statement stmt;
            ResultSet rs;
            
            String pais;
            int numeroCiudades;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    pais = rs.getString("country_code");
                    numeroCiudades = rs.getInt("city_count");
                    
                    System.out.println(pais + " | " + numeroCiudades);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta8(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select CountryCode, count(CountryCode) as numero_idiomas, max(Percentage) as per_language from countrylanguage group by CountryCode";
            Statement stmt;
            ResultSet rs;
            
            String pais;
            int numero_idiomas;
            double porcentaje;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    pais = rs.getString("CountryCode");
                    numero_idiomas = rs.getInt("numero_idiomas");
                    porcentaje = rs.getDouble("per_language");
                    
                    System.out.println(pais + " | " + numero_idiomas + " | " + porcentaje);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta9(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select c.Name as country_Name, ci.Name as city_Name,"
                    + " ci.Population as city_population from city ci join country c on ci.CountryCode = c.Code where ci.Population = "
                    + "(select MAX(Population) from city where CountryCode = c.Code)";
            Statement stmt;
            ResultSet rs;
            
            String pais;
            String ciudad;
            int poblacion;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    pais = rs.getString("country_Name");
                    ciudad = rs.getString("city_Name");
                    poblacion = rs.getInt("city_population");
                    
                    System.out.println(pais + " | " + ciudad + " | " + poblacion);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
        
        public static void consulta10(Connection cn){
            
            long startTime = System.currentTimeMillis();
            
            String consulta = "select c.Name as country_name, ci.Name as city_name, ci.Population as city_population from city ci "
                    + "join country c on ci.CountryCode = c.Code where ci.Population > (select AVG(Population) from city "
                    + "where CountryCode = ci.CountryCode)";
            Statement stmt;
            ResultSet rs;
            
            String pais;
            String ciudad;
            int poblacion;
            
            try{
                stmt = cn.createStatement();
                rs = stmt.executeQuery(consulta);
                
                while(rs.next()){
                    pais = rs.getString("country_Name");
                    ciudad = rs.getString("city_Name");
                    poblacion = rs.getInt("city_population");
                    
                    System.out.println(pais + " | " + ciudad + " | " + poblacion);
                }
                
                long endTime = System.currentTimeMillis();
                long totalTime = endTime - startTime;
                
                System.out.println("Tiempo de respuesta del servidor: " + totalTime + " milisegundos");
                
            }catch(Exception e){
                System.out.println("No se ha podido realizar la consulta");
            }
        }
                
        public static void main(String[] args){
            Connection conexion1 = conectar("sakila");
            Connection conexion2 = conectar("world");
            
            consulta1(conexion1);
            System.out.println();
            consulta2(conexion1);
            System.out.println();
            consulta3(conexion2);
            System.out.println();
            consulta4(conexion2);
            System.out.println();
            
            consulta5(conexion1);
            System.out.println();
            consulta6(conexion1);
            System.out.println();
            consulta7(conexion2);
            System.out.println();
            consulta8(conexion2);
            System.out.println();
            
            consulta9(conexion2);
            System.out.println();
            consulta10(conexion2);
            
            desconectar(conexion1, "sakila");
            desconectar(conexion2, "world");
        }
}
