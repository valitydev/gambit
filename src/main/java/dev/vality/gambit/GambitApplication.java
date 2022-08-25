package dev.vality.gambit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;

@ServletComponentScan
@SpringBootApplication
public class GambitApplication extends SpringApplication {

    public static void main(String[] args) {
        SpringApplication.run(GambitApplication.class, args);
    }

}
