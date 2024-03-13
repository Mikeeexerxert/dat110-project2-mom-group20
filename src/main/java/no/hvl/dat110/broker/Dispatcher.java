package no.hvl.dat110.broker;
import java.util.Collection;
import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;
public class Dispatcher extends Stopable {
	private final Storage storage;
	public Dispatcher(Storage storage) {
		super("Dispatcher");
		this.storage = storage;
	}
	@Override
	public void doProcess() {
		Collection<ClientSession> clients = storage.getSessions();
		Logger.lg(".");
		for (ClientSession client : clients) {
			Message msg = null;
			if (client.hasData()) {
				msg = client.receive();
			}
			// a message was received
			if (msg != null) {
				dispatch(msg);
			}
		}
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	public void dispatch(Message msg) {
		MessageType type = msg.getType();
		// invoke the appropriate handler method
		switch (type) {
		case DISCONNECT:
			onDisconnect((DisconnectMsg) msg);
			break;
		case CREATETOPIC:
			onCreateTopic((CreateTopicMsg) msg);
			break;
		case DELETETOPIC:
			onDeleteTopic((DeleteTopicMsg) msg);
			break;
		case SUBSCRIBE:
			onSubscribe((SubscribeMsg) msg);
			break;
		case UNSUBSCRIBE:
			onUnsubscribe((UnsubscribeMsg) msg);
			break;
		case PUBLISH:
			onPublish((PublishMsg) msg);
			break;
		default:
			Logger.log("broker dispatch - unhandled message type");
			break;
		}
	}
	// called from Broker after having established the underlying connection
	public void onConnect(ConnectMsg msg, Connection connection) {
		storage.addClientSession(msg.getUser(), connection);
		Logger.log("onConnect: " + msg);
	}
	// called by dispatch upon receiving a disconnect message
	public void onDisconnect(DisconnectMsg msg) {
		storage.removeClientSession(msg.getUser());
		Logger.log("onDisconnect: " + msg);
	}
	public void onCreateTopic(CreateTopicMsg msg) {
		storage.createTopic(msg.getTopic());
		Logger.log("onCreateTopic: " + msg);
	}
	public void onDeleteTopic(DeleteTopicMsg msg) {
		storage.deleteTopic(msg.getTopic());
		Logger.log("onDeleteTopic: " + msg);
	}
	public void onSubscribe(SubscribeMsg msg) {
		storage.addSubscriber(msg.getUser(), msg.getTopic());
		Logger.log("onSubscribe: " + msg.getUser() + " subscribed to " + msg.getTopic());
	}
	public void onUnsubscribe(UnsubscribeMsg msg) {
		storage.removeSubscriber(msg.getTopic(), msg.getUser());
		Logger.log("onUnsubscribe: " + msg.getUser() + " unsubscribed from " + msg.getTopic());
	}
	public void onPublish(PublishMsg msg) {
		storage.getSubscribers(msg.getTopic()).forEach(subscriber -> storage.getSession(subscriber).send(msg));
		Logger.log("onPublish: " + msg);
	}
}
