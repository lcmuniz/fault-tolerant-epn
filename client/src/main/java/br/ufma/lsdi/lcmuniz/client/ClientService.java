package br.ufma.lsdi.lcmuniz.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Nats;
import lombok.val;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class ClientService {

    public ClientService() throws Exception {

        configNats();

    }

   private void configNats() throws Exception {
        val connection = Nats.connect();
        val mapper = new ObjectMapper();

        val dispatcher = connection.createDispatcher(msg -> {});
        val subscription = dispatcher.subscribe("MaxTemperature", msg -> {
            try {
                val str = new String(msg.getData(), StandardCharsets.UTF_8);
                val temperature = mapper.readValue(str, MaxTemperature.class);
                System.out.println("Max Temperature: " + temperature.getValue() + " at " + temperature.getTimestamp());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}

