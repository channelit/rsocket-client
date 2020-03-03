package biz.cits.reactive.client.rsocket;

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

@RestController
public class RSocketController {

    private final RSocketRequester rSocketRequester;

    public RSocketController(RSocketRequester rSocketRequester) {
        this.rSocketRequester = rSocketRequester;
    }

    @GetMapping(value = "/messages/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<Message> getMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("messages/" + filter)
                .data(filter)
                .retrieveFlux(Message.class);
    }

    @GetMapping(value = "/camel/{filter}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<Message> getCamelMessages(@PathVariable String filter) {
        return rSocketRequester
                .route("camel/" + filter)
                .data(filter)
                .retrieveFlux(Message.class);
    }

    @GetMapping(value = "/replay/{client}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<Message> replay(@PathVariable String client) {
        return rSocketRequester
                .route("replay/" + client)
                .data(client)
                .retrieveFlux(Message.class);
    }

    @GetMapping(value = "/post", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Mono<String> postMessags() {
        Message m = new Message(MsgGenerator.getMessages(1).get(0).getValue());
        System.out.println(m);
        return rSocketRequester
                .route("post/me")
                .data(m).retrieveMono(String.class);
    }

    @GetMapping(value = "/posts", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<Message> postMessages(@RequestParam int num) {
        Flux<Message> messages = Flux.create(messageFluxSink ->
                MsgGenerator.getMessages(num).forEach(m -> {
                    messageFluxSink.next(new Message(m.getValue()));
                }));
        return rSocketRequester
                .route("posts/me")
                .data(messages)
                .retrieveFlux(Message.class);
    }

}
