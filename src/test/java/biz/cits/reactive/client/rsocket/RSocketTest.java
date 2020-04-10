package biz.cits.reactive.client.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.springframework.http.MediaType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class RSocketTest {

    public static void main(String[] args) {
        TcpClientTransport clientTransport = TcpClientTransport.create("localhost", 7000);
        Mono<RSocket> client = RSocketFactory
                .connect()
                .mimeType(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toString(), String.valueOf(MediaType.APPLICATION_CBOR))
//                .frameDecoder(PayloadDecoder.ZERO_COPY)
                .transport(TcpClientTransport.create("localhost", 7000))
                .start();
//                .block();
//        RSocket client = RSocketFactory.connect().keepAlive().transport(clientTransport).start().block();
        try {
            try {
                Flux<Payload> s = client.block().requestStream(DefaultPayload.create("ABCDE"));
                s
                        .delayElements(Duration.ofMillis(500))
                        .subscribe();
            } finally {
//                client.dispose();
            }
        } finally {
//            client.dispose();
        }
    }
}