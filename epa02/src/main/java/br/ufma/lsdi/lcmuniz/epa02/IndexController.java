package br.ufma.lsdi.lcmuniz.epa02;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    @GetMapping("/")
    public String version() throws Exception {
        return "1.0.0";
    }
}
