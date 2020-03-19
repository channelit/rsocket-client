package biz.cits.reactive.client.rsocket;

import biz.cits.reactive.model.ClientMessage;
import biz.cits.reactive.model.Message;
import biz.cits.reactive.model.MsgGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.reactivestreams.Publisher;
import org.springframework.core.annotation.Order;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@Order(1)
public class RSocketController {

    private final RSocketRequester rSocketRequester;

    public RSocketController(RSocketRequester rSocketRequester) {
        this.rSocketRequester = rSocketRequester;
    }

    @GetMapping(value = "/messages/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("messages/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("camel/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-durable/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelDurableMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-durable/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-durable-direct/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelDurableDirectMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-durable-direct/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-virtual/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelVirtualMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-virtual/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/camel-virtual-direct/{client}/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> getCamelVirtualDirectMessages(@PathVariable String client, @PathVariable String filter) {
        return rSocketRequester
                .route("camel-virtual-direct/" + client + "/" + filter)
                .data(filter)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/replay/{client}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> replay(@PathVariable String client) {
        return rSocketRequester
                .route("replay/" + client)
                .data(client)
                .retrieveFlux(String.class);
    }

    @GetMapping(value = "/post", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Mono<String> postMessags() {
        Map.Entry<String, String> message = MsgGenerator.getMessages(1).get(0);
        Message m = ClientMessage.builder()
                .client(message.getKey())
                .id(UUID.randomUUID())
                .content(message.getValue())
                .messageDateTime(Instant.now()).build();
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        String jsonString = "";
        try {
            jsonString = mapper.writeValueAsString(m);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return rSocketRequester
                .route("post/me")
                .data(jsonString).retrieveMono(String.class);
    }

    @GetMapping(value = "/posts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<String> postMessages(@RequestParam int num) {
        Flux<String> messages = Flux.create(messageFluxSink ->
                MsgGenerator.getMessages(num).forEach(message -> {
                    ClientMessage clientMessage = ClientMessage.builder()
                            .client(message.getKey())
                            .id(UUID.randomUUID().toString())
                            .content(message.getValue())
                            .messageDateTime(Instant.now()).build();
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.registerModule(new JavaTimeModule());
                    String jsonString = "";
                    try {
                        jsonString = mapper.writeValueAsString(clientMessage);

                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    messageFluxSink.next(jsonString);
                }));
        return rSocketRequester
                .route("posts/me")
                .data(messages)
                .retrieveFlux(String.class);
    }


}
