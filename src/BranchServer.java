//import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

public class BranchServer {
	public static BranchHandler handler;
	public static Branch.Processor<BranchHandler> processor;
	public static void main(String[] args){
		int portNumber;
		String branchName;
		if(args.length != 2){
			System.out.println("Please provide server name and port number in order");
			System.exit(0);
		}
		try{
			portNumber = Integer.parseInt(args[1]);
			branchName = args[0];
			handler = new BranchHandler(branchName);
			processor = new Branch.Processor<BranchHandler>(handler);
			//BasicConfigurator.configure();
			simple(processor,portNumber);
		}
		catch(NumberFormatException e){
			System.out.println("Please provide server name and port number in order");
			System.exit(0);
		}
	}

	public static void simple(Branch.Processor<BranchHandler> processor, int portNumber){
		//System.out.println("port number is "+portNumber);
		try{
			TServerTransport serverTransport = new TServerSocket(portNumber);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
			System.out.println("Branch server started in port number "+portNumber);
			server.serve();
		} 
		catch(TTransportException e){
			System.out.println("Provided port number is in use. Please provide different port number");
			System.exit(0);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
