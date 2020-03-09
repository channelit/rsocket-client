package biz.cits.reactive.client.rsocket;

import biz.cits.reactive.model.ClientMessage;
import biz.cits.reactive.model.Message;
import biz.cits.reactive.model.MsgGenerator;
import org.reactivestreams.Publisher;
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
public class RSocketController {

    private final RSocketRequester rSocketRequester;

    public RSocketController(RSocketRequester rSocketRequester) {
        this.rSocketRequester = rSocketRequester;
    }

    @GetMapping(value = "/messages/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<?> getMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("messages/" + filter)
                .data(filter)
                .retrieveFlux(ClientMessage.class);
    }

    @GetMapping(value = "/camel/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<ClientMessage> getCamelMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("camel/" + filter)
                .data(filter)
                .retrieveFlux(ClientMessage.class);
    }

    @GetMapping(value = "/replay/{client}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<ClientMessage> replay(@PathVariable String client) {
        return rSocketRequester
                .route("replay/" + client)
                .data(client)
                .retrieveFlux(ClientMessage.class);
    }

    @GetMapping(value = "/post", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Mono<String> postMessags() {
        Map.Entry<String, String> message = MsgGenerator.getMessages(1).get(0);
        Message m = ClientMessage.builder()
                .client(message.getKey())
                .id(UUID.randomUUID())
                .content(message.getValue())
                .messageDateTime(Instant.now()).build();
        return rSocketRequester
                .route("post/me")
                .data(m).retrieveMono(String.class);
    }

    @GetMapping(value = "/posts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<ClientMessage> postMessages(@RequestParam int num) {
        Flux<Message> messages = Flux.create(messageFluxSink ->
                MsgGenerator.getMessages(num).forEach(message -> {
                    ClientMessage clientMessage = ClientMessage.builder()
                            .client(message.getKey())
                            .id(UUID.randomUUID().toString())
                            .content(message.getValue())
                            .messageDateTime(Instant.now()).build();
                    messageFluxSink.next(clientMessage);
                }));
        return rSocketRequester
                .route("posts/me")
                .data(messages)
                .retrieveFlux(ClientMessage.class);
    }


}
