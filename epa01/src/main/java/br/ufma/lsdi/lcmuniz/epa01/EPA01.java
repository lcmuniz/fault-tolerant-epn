package br.ufma.lsdi.lcmuniz.epa01;

import com.espertech.esper.client.EPServiceProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.val;
import org.springframework.stereotype.Service;
import com.espertech.esper.client.EPServiceProviderManager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

@Service
public class EPA01 {

    private Connection connection;

    private EPServiceProvider provider;
    private boolean connected = false;

    Queue<AvgTemperature> queue = new LinkedList<>();

    public EPA01() throws Exception {

        bootstrap();

        configPublishQueue();

        configCEP();

        configNats();

    }

    private void bootstrap() throws Exception {

        val options = new Options.Builder()
                .server("nats://localhost:4222")
                .connectionListener(new ConnectionListener() {
                    @Override
                    public void connectionEvent(Connection connection, Events events) {
                        if (events == Events.DISCONNECTED) {
                            connected = false;
                        }
                        else if (events == Events.CLOSED) {
                            System.out.println("CLOSED");
                        }
                        else if (events == Events.CONNECTED || events == Events.RECONNECTED) {
                            connected = true;
                        }
                        else {
                            System.out.println(events);
                        }
                    }
                })
                .build();

        connection = Nats.connect(options);

    }

    private void configCEP() throws Exception {

        provider = EPServiceProviderManager.getDefaultProvider();
        provider.getEPAdministrator().getConfiguration().addEventType(Temperature.class);
        provider.getEPAdministrator().getConfiguration().addEventType(AvgTemperature.class);

        val statement = provider.getEPAdministrator().createEPL("select * from AvgTemperature");
        statement.addListener((eventBeans, eventBeans1) -> {
            val temperature = (AvgTemperature) eventBeans[0].getUnderlying();
            //System.out.println("AvgTemperature received from Cep: " + temperature);
            queue.add(temperature);
            //System.out.println("AvgTemperature published to Nat: " + temperature);
        });

        provider.getEPAdministrator().createEPL("insert into AvgTemperature select avg(value) as value, max(timestamp) as timestamp from Temperature.win:time_batch(10 sec)");
    }

    private void configPublishQueue() {
        new Thread(() -> {
            while (true) {
                System.out.println(connected);
                if (connected && queue.size() > 0) {
                    try {
                        val mapper = new ObjectMapper();
                        val str = mapper.writeValueAsString(queue.remove());
                        connection.publish("AvgTemperature", str.getBytes(StandardCharsets.UTF_8));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
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
