package com.collector.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.FixedSubscriberChannel;
import org.springframework.integration.ip.tcp.TcpInboundGateway;
import org.springframework.integration.ip.tcp.TcpReceivingChannelAdapter;
import org.springframework.integration.ip.tcp.TcpSendingMessageHandler;
import org.springframework.integration.ip.tcp.connection.AbstractClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNioClientConnectionFactory;
import org.springframework.integration.ip.tcp.connection.TcpNioServerConnectionFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Scanner;

@SpringBootApplication
@EnableScheduling
public class DemoApplication {
	@Value("${server.client.port}")
	private int port;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Autowired
	private Gateway gateway;

	@Scheduled(fixedDelay = 10000L)
	public void sendMessageJob() throws FileNotFoundException {
		//gateway
		String fileName = "files/archivo1.txt";
		Scanner scan = new Scanner(new File(fileName));
		while(scan.hasNext()){
			String msj = scan.nextLine();
			gateway.sendMessage(msj);
		}
	}

	//Server
	@Bean
	public AbstractServerConnectionFactory serverConnectionFactory() {
		return new TcpNioServerConnectionFactory(port);
	}

	@Bean
	public MessageChannel requestChannel() {
		return new DirectChannel(); //canal de solicitud
	}

	@Bean
	public TcpInboundGateway tcpInboundGateway() { //puerta de enlace de entrada tcp
		TcpInboundGateway tcpInboundGateway = new TcpInboundGateway();
		tcpInboundGateway.setConnectionFactory(serverConnectionFactory());
		tcpInboundGateway.setRequestChannel(requestChannel());
		return tcpInboundGateway;
	}

	@Transformer(inputChannel = "requestChannel", outputChannel = "requestChannel2") //Entrada -> requestChannel, salida _> rtequestChannel2
	public String serverConvert(byte[] bytes) { //Convertir de bytes a Stirng
		return new String(bytes);
	}

	@ServiceActivator(inputChannel = "requestChannel2") //entrada -> requestChannel2
	public String handleRequest(String msg) throws Exception {
		System.out.println ("El servidor maneja el mensaje de solicitud = " + msg); // servidor maneja
		//Object mapper para convertir objetos de java a JSON
		//ObjectMapper objectMapper = new ObjectMapper();
		//Map map = objectMapper.readValue(msg, Map.class);
		//map.put("result", "is processed");
		//return objectMapper.writeValueAsString(map);
		return msg;
	}

	//Client

	@Bean
	public AbstractClientConnectionFactory clientConnectionFactory() {
		return new TcpNioClientConnectionFactory("localhost", 5678);
	}

	@Component
	@MessagingGateway(defaultRequestChannel = "sendMessageChannel")
	public interface Gateway {
		void sendMessage(String message);
	}

	@Bean
	@ServiceActivator(inputChannel = "sendMessageChannel")
	public TcpSendingMessageHandler tcpSendingMessageHandler() {
		TcpSendingMessageHandler tcpSendingMessageHandler = new TcpSendingMessageHandler();
		tcpSendingMessageHandler.setConnectionFactory(clientConnectionFactory());
		return tcpSendingMessageHandler;
	}

	@Bean
	public TcpReceivingChannelAdapter tcpReceivingChannelAdapter() {
		TcpReceivingChannelAdapter tcpReceivingChannelAdapter = new TcpReceivingChannelAdapter();
		tcpReceivingChannelAdapter.setConnectionFactory(clientConnectionFactory());
		tcpReceivingChannelAdapter.setOutputChannelName("outputChannel");
		return tcpReceivingChannelAdapter;
	}

	@Transformer(inputChannel = "outputChannel", outputChannel = "outputChannel2")
	public String clientConvert(byte[] bytes) {
		return new String(bytes);
	}
/*
	@Bean
	public MessageChannel outputChannel2() {
		return new FixedSubscriberChannel(msg -> {
			 System.out.println ("Mensaje de respuesta del identificador del cliente =" + msg.getPayload ()); // identificador del cliente
		});
	}

	@Bean
	@ServiceActivator(inputChannel = "outputChannel2")
	public MessageHandler handleResponse() {
		return msg -> {
			 System.out.println ("Mensaje de respuesta del identificador del cliente =" + msg.getPayload ()); // identificador del cliente
		};
	}*/

	@ServiceActivator(inputChannel = "outputChannel2")
	public void handleResponse(String msg) throws Exception {
		System.out.println ("Mensaje de respuesta del identificador del cliente =" + msg.toUpperCase().concat(" mensaje recibido por el cliente")); // identificador del cliente
	}
}
