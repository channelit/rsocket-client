package biz.cits.reactive.client.rsocket;

import com.google.auto.value.AutoValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Order(3)
@EnableScheduling
public class RSocketRunner {

    final RSocketRequester rSocketRequester;

    public RSocketRunner(RSocketRequester rSocketRequester) {
        this.rSocketRequester = rSocketRequester;
    }

    @Scheduled(fixedDelay = 30000, initialDelay = 30000)
    public void processMessages() {
        System.out.println("Starting processing");
        rSocketRequester
                .route("camel-virtual-direct/me/ABCDE")
                .data("ABCDE")
                .retrieveFlux(String.class).doOnNext(System.out::println).subscribe();
    }
}
