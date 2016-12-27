import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class BranchHandler implements Branch.Iface {
	private int branchBalance;
	private String branchName;
	private TransferMessage transferMessage;
	private int branchCounter = 0;
	private BranchID branchId;
	private Hashtable<String,List> incomingChannels = new Hashtable<String,List>();
	Vector<Integer> localState = new Vector<Integer>();
	
	Hashtable<String,Integer> receivedMessageIdTable = new Hashtable<String,Integer>();
	Hashtable<String,Integer> sentMessageIdListTable = new Hashtable<String,Integer>();
	
	

	private Hashtable<Integer,LocalSnapshot> completeSnapshots = new Hashtable<Integer,LocalSnapshot>();
	private boolean transferFlag = false;
	private List<BranchID> branchesList = new ArrayList<BranchID>();
	@Override
	public void initBranch(int balance, List<BranchID> all_branches) throws SystemException, TException {
		System.out.println("Branch name is "+branchName);
		System.out.println("Balance is "+balance);
		this.setBranchBalance(balance);
		this.branchId = new BranchID();
		for(BranchID branch:all_branches){
			if(branch.getName().equals(branchName)){
				branchId = branch;
			}
			this.receivedMessageIdTable.put(branch.getName(), 0);
		}
		this.branchesList = all_branches;
		Runnable transferMoneyThread = new Runnable(){
			public void run(){
				TransferMoney();
			}
		};
		new Thread(transferMoneyThread).start();
	}

	public void TransferMoney(){ 
		TransferMessage transferMessage = new TransferMessage();
		int messageId = 0;
		TTransport transport = null;
		int randomNumber = 0;
		while(true){
			while(isTransferFlag()){
				
			}
			try {
				Thread.sleep(500);
//				if(randomNumber == this.getBranchesList().size()){
//					randomNumber = 1;
//				}
				Random random = new Random();
				randomNumber = random.nextInt(branchesList.size()-1) + 0;
				if(this.getBranchesList().get(randomNumber).getName().equals(
						this.getBranchName())){
					//randomNumber+=1;
					continue;
				}
				transport = new TSocket(this.getBranchesList().get(randomNumber).getIp(), 
						this.getBranchesList().get(randomNumber).getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				if(getBranchBalance()<100){
					break;
				}
				Random randomMoney = new Random();
				int percentage = randomMoney.nextInt(5)+1;
				int amountSent = ((percentage*this.getBranchBalance())/100);
				transferMessage.setAmount(amountSent);
				transferMessage.setOrig_branchId(branchId);
				this.setBranchBalance(this.getBranchBalance()-amountSent);
				Branch.Client destinationBranch = new Branch.Client(protocol);
				String destinationBranchName = this.getBranchesList().get(randomNumber).getName();
				if(sentMessageIdListTable.containsKey(destinationBranchName)){
					messageId = this.sentMessageIdListTable.get(destinationBranchName) + 1;
					this.sentMessageIdListTable.put(destinationBranchName, messageId);
				}
				else{
					messageId = 1;
					this.sentMessageIdListTable.put(destinationBranchName, 1);
				}
				destinationBranch.transferMoney(transferMessage, messageId);
			} catch (SystemException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			} 
			catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			transport.close();
			//randomNumber++;
		}

	}

	@Override
	public void transferMoney(TransferMessage messageIN, int messageIdIN) throws SystemException, TException {
		while(messageIdIN - this.receivedMessageIdTable.get(messageIN.getOrig_branchId().getName()) != 1){
			System.out.println("message id sequence is missing in tranferMoney "
					+messageIdIN+" this.receivedMessageIdTable.get(messageIN.getOrig_branchId().getName())");
		}
		synchronized(this){
			this.setBranchBalance(this.getBranchBalance() + messageIN.getAmount());
			this.receivedMessageIdTable.put(messageIN.getOrig_branchId().getName(),
					(receivedMessageIdTable.get(messageIN.getOrig_branchId().getName()) + 1));
		}
//		recordOwnState(messageIN.getAmount());
	}

//	public synchronized void recordOwnState(int amount){
//		localState.add(amount);
//	}

	@Override
	public void initSnapshot(int snapshotId) throws SystemException, TException {
		System.out.println("snapshot initiated with id "+snapshotId);
		int messageId;
		//save local state
		
		this.setTransferFlag(true);
		LocalSnapshot localSnapshot = new LocalSnapshot();
		localSnapshot.setBalance(this.getBranchBalance());
		localSnapshot.setSnapshotId(snapshotId);
		//localSnapshot.setMessages(localState);
		this.localState.clear();
		System.out.println("init check "+branchBalance);
		completeSnapshots.put(snapshotId, localSnapshot);
		//send marker messages to other branches
		TTransport transport = null;
		for(BranchID branchID:branchesList){
			//System.out.println("Inside for loop of init snapshot");
			if(branchID.getName().equals(this.branchName)){
				continue;
			}
			else{
				transport = new TSocket(branchID.getIp(), branchID.getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				Branch.Client destinationMarkerBranch = new Branch.Client(protocol);
				if(sentMessageIdListTable.containsKey(branchID.getName())){
					messageId = this.sentMessageIdListTable.get(branchID.getName()) + 1;
					this.sentMessageIdListTable.put(branchID.getName(), messageId);
				}
				else{
					messageId = 1;
					this.sentMessageIdListTable.put(branchID.getName(), 1);
				}
				destinationMarkerBranch.Marker(branchId, snapshotId, messageId);
			}
		}
		this.setTransferFlag(false);
	}

	@Override
	public void Marker(BranchID branchIdIN, int snapshotIdIN, int messageIdIN) throws SystemException, TException {
		int messageId;
		while((messageIdIN - this.receivedMessageIdTable.get(branchIdIN.getName()) != 1)){
			System.out.println("Missing message id sequence");
		}
		this.setTransferFlag(true);
		//synchronized(this){
		if(!completeSnapshots.containsKey(snapshotIdIN)){
			//record its own state
			//this.localState.clear();
			LocalSnapshot localSnapshot = new LocalSnapshot();
			localSnapshot.setBalance(this.getBranchBalance());
			localSnapshot.setSnapshotId(snapshotIdIN);
			//localSnapshot.setMessages(localState);
			this.localState.clear();
			this.completeSnapshots.put(snapshotIdIN, localSnapshot);

			//start sending marker messages and start recording channels
			TTransport transport = null;
			
				for(BranchID branch:branchesList){
					if(branch.getName().equals(branchName)){
						continue;
					}
					transport = new TSocket(branch.getIp(), branch.getPort());
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					Branch.Client destinationMarkerBranch = new Branch.Client(protocol);
					
						if(sentMessageIdListTable.containsKey(branch.getName())){
							messageId = this.sentMessageIdListTable.get(branch.getName()) + 1;
							this.sentMessageIdListTable.put(branch.getName(), messageId);
						}
						else{
							messageId = 1;
							this.sentMessageIdListTable.put(branch.getName(), 1);
						}
					
					destinationMarkerBranch.Marker(branchId, snapshotIdIN, messageId);
					//start recording
					//				for(BranchID branch1:branchesList){
					//					if(branch1.getName().equals(branchName) || branch1.getName().equals(branchId.getName())){
					//						continue;
					//					}
					//				incomingChannels.put(snapshotId+branch1.getName(), new ArrayList<Integer>());
					//				}
				}
			
		}

		else{ 
			//check whether channel exists in hashtable
			//			if(incomingChannels.containsKey(branchId)){
			//				incomingChannels.put(snapshotId+branchId.getName(), localState);
			//				//completeSnapshots.put(snapshotId, incomingChannels.get(snapshotId+branchId.getName());
			//			}
			this.localState.clear();
			this.completeSnapshots.get(snapshotIdIN).setMessages(localState);
		}
		this.receivedMessageIdTable.put(branchIdIN.getName(), (1 + this.receivedMessageIdTable.get(branchIdIN.getName())));
		//}
		this.setTransferFlag(false);
	}

	@Override
	public LocalSnapshot retrieveSnapshot(int snapshotId) throws SystemException, TException {
		System.out.println("reached retrieveSnapshot");
		System.out.println("size of completesnapshots "+completeSnapshots.size());
		System.out.println(completeSnapshots.get(snapshotId));
		return completeSnapshots.get(snapshotId);
	}
	
	public synchronized boolean isTransferFlag() {
		return transferFlag;
	}

	public synchronized void setTransferFlag(boolean transferFlag) {
		this.transferFlag = transferFlag;
	}

	public BranchHandler(String branchNameIN){
		branchName = branchNameIN;
	}

	public String getBranchName() {
		return branchName;
	}

	public void setBranchName(String branchName) {
		this.branchName = branchName;
	}

	public List<BranchID> getBranchesList() {
		return branchesList;
	}

	public void setBranchesList(List<BranchID> branchesList) {
		this.branchesList = branchesList;
	}

	public synchronized int getBranchBalance() {
		return branchBalance;
	}
	public synchronized void setBranchBalance(int branchBalanceIN) {
		this.branchBalance = branchBalanceIN;
	}
}
