package com.example.service;

import com.mongodb.client.model.changestream.OperationType;
import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.Flow;

@ApplicationScoped
public class CDCService {

    private final ReactiveMongoClient mongoClient;
    private final EventService eventService;

    public CDCService(ReactiveMongoClient mongoClient, EventService eventService) {
        this.mongoClient = mongoClient;
        this.eventService = eventService;
    }

    @PostConstruct
    private void initChangeDateObserver() {
        mongoClient.getDatabase("test")
                .getCollection("events")
                .watch()
                .onItem()
                .transformToUni(it -> {
                    var type = it.getOperationType();
                    if (type.equals(OperationType.INSERT)) {
                        final var document = it.getFullDocument();
                        System.out.println("document.toJson() = " + document.toJson());
                        final var event = document.getString("event");
                        final var timeStamp = Timestamp.valueOf(document.get("soeTime", LocalDateTime.class)).toInstant();
                        final var topic = document.getString("topic");
                        final var id = document.getObjectId("_id").toHexString();
                        return eventService.publishEvent(event, topic, id, timeStamp);
                    }
                    return Uni.createFrom().nullItem();

                }).merge()
                .subscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        System.out.println("Start Subscription");
                    }

                    @Override
                    public void onNext(RecordMetadata item) {

                    }

                    @Override
                    public void onError(Throwable throwable) {

                    }

                    @Override
                    public void onComplete() {
                        System.out.println("Completed");

                    }
                });

    }
}

