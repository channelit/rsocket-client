package biz.cits.reactive.client.rsocket;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.netty.client.TcpClientTransport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.codec.CharSequenceEncoder;
import org.springframework.core.codec.StringDecoder;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;

@Configuration
public class RSocketConfig {

    @Autowired
    private RSocketRequester.Builder builder;


    @Bean
    RSocket rSocket() {
        return RSocketFactory
                .connect()
//                .dataMimeType(MimeTypeUtils.APPLICATION_JSON_VALUE)
                .mimeType(WellKnownMimeType.MESSAGE_RSOCKET_ROUTING.toString(), String.valueOf(MediaType.APPLICATION_CBOR))
                .frameDecoder(PayloadDecoder.ZERO_COPY)
                .transport(TcpClientTransport.create("localhost", 7000))
                .start()
                .block();
    }

//    @Bean
//    RSocketStrategies strategies() {
//        return RSocketStrategies.builder()
//                .decoder(StringDecoder.textPlainOnly())
//                .encoder(CharSequenceEncoder.allMimeTypes())
//                .dataBufferFactory(new DefaultDataBufferFactory(true))
//                .build();
//    }

    @Bean
    RSocketRequester rSocketRequester(RSocketStrategies strategies) {
        return RSocketRequester.builder()
                .rsocketFactory(factory -> factory
                        .dataMimeType(MimeTypeUtils.ALL_VALUE)
                        .frameDecoder(PayloadDecoder.ZERO_COPY))
                .rsocketStrategies(strategies)
                .connect(TcpClientTransport.create("localhost", 7000))
                .retry().block();
    }

}
