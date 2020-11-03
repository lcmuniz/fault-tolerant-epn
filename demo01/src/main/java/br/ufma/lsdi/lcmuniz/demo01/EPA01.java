package br.ufma.lsdi.lcmuniz.demo01;

import com.espertech.esper.client.EPServiceProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Nats;
import lombok.val;
import org.springframework.stereotype.Service;
import com.espertech.esper.client.EPServiceProviderManager;

import java.nio.charset.StandardCharsets;

@Service
public class EPA01 {

    private EPServiceProvider provider;

    public EPA01() throws Exception {

        configCEP();

        val connection = Nats.connect();
        val mapper = new ObjectMapper();

        val dispatcher = connection.createDispatcher(msg -> {});
        val subscription = dispatcher.subscribe("temperature", msg -> {
            try {
                val str = new String(msg.getData(), StandardCharsets.UTF_8);
                val temperature = mapper.readValue(str, Temperature.class);
                provider.getEPRuntime().sendEvent(temperature);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void configCEP() {
        provider = EPServiceProviderManager.getDefaultProvider();
        provider.getEPAdministrator().getConfiguration().addEventType(Temperature.class);
        val statement = provider.getEPAdministrator().createEPL("select * from Temperature");
        statement.addListener((eventBeans, eventBeans1) -> {
            System.out.println(">>>>>>>>>>>>>");
            val temperature = (Temperature) eventBeans[0].getUnderlying();
            System.out.println(temperature.getValue());
        });
    }

}
