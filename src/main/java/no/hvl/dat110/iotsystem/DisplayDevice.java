package no.hvl.dat110.iotsystem;
import no.hvl.dat110.client.Client;
import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;
public class DisplayDevice {
	private static final int COUNT = 10;
	public static void main (String[] args) {
		System.out.println("Display starting ...");
		Client client = new Client("display", Common.BROKERHOST, Common.BROKERPORT);
		client.connect();
		client.createTopic(Common.TEMPTOPIC);
		client.subscribe(Common.TEMPTOPIC);
		for (int i = 0; i < COUNT; i++) {
			Message message = client.receive();
			if (message instanceof PublishMsg) {
				PublishMsg publishMsg = (PublishMsg) message;
				System.out.println("Display: " + publishMsg.getMessage());
			}
		}
		client.unsubscribe(Common.TEMPTOPIC);
		client.disconnect();
		System.out.println("Display stopping ... ");
	}
}
