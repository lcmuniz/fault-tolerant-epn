package br.ufma.lsdi.lcmuniz.monitor;

import com.espertech.esper.client.EPServiceProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.val;
import org.springframework.stereotype.Service;
import com.espertech.esper.client.EPServiceProviderManager;

import java.nio.charset.StandardCharsets;

@Service
public class MonitorService {

    private EPServiceProvider provider;

    public MonitorService() throws Exception {

        configNats();

    }

    private void configNats() throws Exception {

        val connection = Nats.connect();
        val mapper = new ObjectMapper();

        val dispatcher = connection.createDispatcher(msg -> {});
        val subscription = dispatcher.subscribe("Temperature", msg -> {
            try {
                val str = new String(msg.getData(), StandardCharsets.UTF_8);
                val temperature = mapper.readValue(str, Temperature.class);
                //System.out.println("Temperature received from Nat: " + temperature);
                provider.getEPRuntime().sendEvent(temperature);
                //System.out.println("Temperature published to Cep: " + temperature);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
