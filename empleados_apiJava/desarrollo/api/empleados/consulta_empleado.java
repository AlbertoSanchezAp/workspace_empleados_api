package desarrollo.api.empleados;


import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import com.ibm.broker.javacompute.MbJavaComputeNode;
import com.ibm.broker.plugin.MbElement;
import com.ibm.broker.plugin.MbException;
import com.ibm.broker.plugin.MbJSON;
import com.ibm.broker.plugin.MbMessage;
import com.ibm.broker.plugin.MbMessageAssembly;
import com.ibm.broker.plugin.MbOutputTerminal;
import com.ibm.broker.plugin.MbUserException;

public class consulta_empleado extends MbJavaComputeNode {
	public String url = "jdbc:mysql://localhost:3306/desarrolloApp?allowPublicKeyRetrieval=true";

	public void evaluate(MbMessageAssembly inAssembly) throws MbException {
		MbOutputTerminal out = getOutputTerminal("out");

		MbMessage inMessage = inAssembly.getMessage();
		MbMessageAssembly outAssembly = null;
		try {

			MbMessage outMessage = new MbMessage();
			outAssembly = new MbMessageAssembly(inAssembly, outMessage);
			MbElement outRoot = outMessage.getRootElement();

			MbElement outHTTPRequest = outRoot.createElementAsLastChild("HTTPRequestHeader");
			outHTTPRequest.createElementAsLastChild(MbElement.TYPE_NAME_VALUE, "Content-Type", "application/json; charset=utf-8"); //charset=utf-8 || charset=ISO-8859-1

			MbElement outJsonRoot = outRoot.createElementAsLastChild(MbJSON.PARSER_NAME);
			MbElement outJsonData = outJsonRoot.createElementAsLastChild(MbElement.TYPE_NAME, MbJSON.DATA_ELEMENT_NAME, null);

			Properties props = new Properties();
			MbElement entrada = inMessage.getRootElement().getLastChild().getFirstElementByPath("/JSON/Data").getFirstChild();
			
			
			while(entrada != null ) {			
				props.setProperty( entrada.getName(), entrada.getValueAsString() );	
				entrada = entrada.getNextSibling();
			} 
			MbElement ArrayEmpleado=null,ObjectoEmpleado=null;
			
			CallableStatement cStmt=null;
			Connection conn = null;
			ResultSet rs=null;
			
			try
			   {
				
			     Class.forName("com.mysql.jdbc.Driver");
			     conn = DriverManager.getConnection(url,"root","usrPassw0rd");
			     cStmt = conn.prepareCall("{call sp_consulta_empleado()}");
	        	   	  
	             cStmt.execute();
	             rs=cStmt.getResultSet();
	            
	             ArrayEmpleado= outJsonData.createElementAsLastChild(MbJSON.ARRAY,"Empleados",null);
	             while(rs.next()){
	            	 ObjectoEmpleado= ArrayEmpleado.createElementAsLastChild(MbJSON.OBJECT,"",null);
	            	 ObjectoEmpleado.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"Empleado",rs.getInt(1));
	            	 ObjectoEmpleado.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"Nombre",rs.getString(2));
	            	 ObjectoEmpleado.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"Edad",rs.getInt(3));
	            	 ObjectoEmpleado.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"Sexo",rs.getString(4));
	            	 ObjectoEmpleado.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"Rol",rs.getString(5));
	            	 ObjectoEmpleado.createElementAsLastChild(MbElement.TYPE_NAME_VALUE,"Tipo",rs.getString(6));
			     } 
	             
	             
			   }catch(Exception e){
			    throw e;
			   }finally{
			     cStmt.close();
			     conn.close();
			   }
			
			// End of user code
			// ----------------------------------------------------------
		} catch (MbException e) {
			// Re-throw to allow Broker handling of MbException
			throw e;
		} catch (RuntimeException e) {
			// Re-throw to allow Broker handling of RuntimeException
			throw e;
		} catch (Exception e) {
			// Consider replacing Exception with type(s) thrown by user code
			// Example handling ensures all exceptions are re-thrown to be handled in the flow
			throw new MbUserException(this, "evaluate()", "", "", e.toString(),
					null);
		}
		// The following should only be changed
		// if not propagating message to the 'out' terminal
		out.propagate(outAssembly);

	}
}
