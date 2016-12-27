import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class BankController {
	public static void main(String[] args){
		Scanner inputSource = null;
		int totalBranches = 0;
		int totalBalance = 0;
		if(args.length != 2){
			System.out.println("Please provide total amount and file name containing branches in order");
			System.exit(0);
		}
		try {
			totalBalance = Integer.parseInt(args[0]);
			inputSource = new Scanner(new File(args[1]));
		} catch (FileNotFoundException e1) {
			System.out.println("Specified file didn't found");
			System.exit(0);
		} catch (NumberFormatException e) {
			System.out.println("Please provide total amount and file name containing branches in order");
			System.exit(0);
		}
		List<BranchID> branchesList = new ArrayList<BranchID>();
		while(true){
			if(!inputSource.hasNext()){
				break;
			}
			String eachLine = inputSource.nextLine();
			String eachLineContent[]=eachLine.split("\\s");
			BranchID branchDetails= new BranchID();
			branchDetails.setName(eachLineContent[0]);
			branchDetails.setIp(eachLineContent[1]);
			branchDetails.setPort(Integer.parseInt(eachLineContent[2]));
			branchesList.add(branchDetails);
			totalBranches++;
		}
		int initialBranchBalance = totalBalance/totalBranches;
		TTransport transport=null;
		Branch.Client client = null;
		for(BranchID branch:branchesList){
			try {
				transport = new TSocket(branch.getIp(), branch.getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				client = new Branch.Client(protocol);
				client.initBranch(initialBranchBalance, branchesList);
				transport.close();
			} catch(Exception e){
				e.printStackTrace();
			}
		}

		//starting chandy-lamport algm
		int snapshotId = 0;
		int randomNumber;
		Hashtable<Integer,List<LocalSnapshot>> completeSnapshots = new Hashtable<Integer,List<LocalSnapshot>>();
		try {
			while(snapshotId < branchesList.size()){
				//to start snapshot
				Random rand = new Random();
				randomNumber = rand.nextInt(branchesList.size()-1) + 0;
				transport = new TSocket(branchesList.get(randomNumber).getIp(), branchesList.get(randomNumber).getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				client = new Branch.Client(protocol);
				System.out.println("Waiting for some time to initiate snapshot.");
				Thread.sleep(5000);
				client.initSnapshot(snapshotId);
				transport.close();
				Thread.sleep(6000);
				//to retrieve snapshot
				System.out.println("Snapshot for snapshot id "+snapshotId);
				for(BranchID branch:branchesList){
					LocalSnapshot localSnapshot = new LocalSnapshot();
					transport = new TSocket(branch.getIp(), branch.getPort());
					transport.open();
					protocol = new TBinaryProtocol(transport);
					client = new Branch.Client(protocol);
					localSnapshot = client.retrieveSnapshot(snapshotId);
					if(localSnapshot == null){
						System.out.println("Something went wrong while retrieveing snapshot");
						System.exit(0);
					}
					System.out.println("Snapshot taken at "+branch.getName());
					System.out.println(localSnapshot);				
					transport.close();
				}

				snapshotId++;
			}

		} catch (SystemException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
