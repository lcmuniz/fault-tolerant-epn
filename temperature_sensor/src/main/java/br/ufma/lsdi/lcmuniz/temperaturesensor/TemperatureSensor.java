package br.ufma.lsdi.lcmuniz.temperaturesensor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Nats;
import lombok.val;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Random;

@Service
public class TemperatureSensor {

    public TemperatureSensor() throws Exception {
        val connection = Nats.connect();
        val random = new Random();
        val mapper = new ObjectMapper();
        new Thread(() -> {
            try {
                while(true) {
                    val temperature = new Temperature();
                    temperature.setTimestamp(new Date());
                    temperature.setValue(25 + random.nextInt(20));
                    val str = mapper.writeValueAsString(temperature);
                    connection.publish("Temperature", str.getBytes(StandardCharsets.UTF_8));
                    //System.out.println("Temperature published to Nat: " + temperature);
                    Thread.sleep(5000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

}
