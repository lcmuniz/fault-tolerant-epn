package br.ufma.lsdi.lcmuniz.demo01;

import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import lombok.val;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;

@RestController
public class IndexController {

    @GetMapping("/")
    public String version() throws Exception {
        val connection = Nats.connect();
        connection.publish("topic1", "olá pessoal".getBytes(StandardCharsets.UTF_8));
        return "1.0.0";
    }
}
