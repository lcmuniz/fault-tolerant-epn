package br.ufma.lsdi.lcmuniz.epa02;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.val;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;

@Service
public class EPA02 {

    Connection connection;

    private boolean connected = false;

    Queue<MaxTemperature> queue = new LinkedList<>();

    private EPServiceProvider provider;

    public EPA02() throws Exception {

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
        val mapper = new ObjectMapper();

        provider = EPServiceProviderManager.getDefaultProvider();
        provider.getEPAdministrator().getConfiguration().addEventType(AvgTemperature.class);
        provider.getEPAdministrator().getConfiguration().addEventType(MaxTemperature.class);
        val statement = provider.getEPAdministrator().createEPL("select * from MaxTemperature");
        statement.addListener((eventBeans, eventBeans1) -> {
            val temperature = (MaxTemperature) eventBeans[0].getUnderlying();
            //System.out.println("MaxTemperature received from Cep: " + temperature);
            queue.add(temperature);
            //System.out.println("MaxTemperature received to Nat: " + temperature);
        });

        provider.getEPAdministrator().createEPL("insert into MaxTemperature select max(value) as value, max(timestamp) as timestamp from AvgTemperature.win:time_batch(10 sec)");
    }

    private void configNats() throws Exception {
        val connection = Nats.connect();
        val mapper = new ObjectMapper();

        val dispatcher = connection.createDispatcher(msg -> {});
        val subscription = dispatcher.subscribe("AvgTemperature", msg -> {
            try {
                val str = new String(msg.getData(), StandardCharsets.UTF_8);
                val temperature = mapper.readValue(str, AvgTemperature.class);
                //System.out.println("AvgTemperature received from Nat: " + temperature);
                provider.getEPRuntime().sendEvent(temperature);
                //System.out.println("AvgTemperature received to Cep: " + temperature);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void configPublishQueue() {
        new Thread(() -> {
            while (true) {
                System.out.println(connected);
                if (connected && queue.size() > 0) {
                    try {
                        val mapper = new ObjectMapper();
                        val str = mapper.writeValueAsString(queue.remove());
                        connection.publish("MaxTemperature", str.getBytes(StandardCharsets.UTF_8));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

}

