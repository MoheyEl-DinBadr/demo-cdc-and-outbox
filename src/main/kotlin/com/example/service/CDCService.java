package com.example.service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.conversions.Bson;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Startup
@ApplicationScoped
@IfBuildProperty(name = "enable.cdc", stringValue = "true")
public class CDCService {

    private final MongoClient mongoClient;
    private final EventService eventService;

    public CDCService(MongoClient mongoClient, EventService eventService) {
        this.mongoClient = mongoClient;
        this.eventService = eventService;
    }

    @PostConstruct
    private void initChangeDateObserver() {
        List<Bson> pipeline = Arrays.asList(
                Aggregates.match(
                        Filters.in("operationType",
                                Arrays.asList("insert"))));
        mongoClient.getDatabase("test")
                .getCollection("events")
                .watch(pipeline)
                .forEach(eventDocument -> {
                    final var document = eventDocument.getFullDocument();
                    System.out.println("document.toJson() = " + document.toJson());
                    final var event = document.getString("event");
                    final var timeStamp = document.get("soeTime", Date.class).toInstant();
                    final var topic = document.getString("topic");
                    final var id = document.getObjectId("_id");
                    final var idString = id.toHexString();
                    eventService.publishEvent(event, topic, idString, timeStamp)
                            .log()
                            .flatMap(metaData -> eventService.updateDocument(id, event, topic, LocalDateTime.ofInstant(timeStamp, ZoneOffset.UTC), true))
                            .await().indefinitely();
                });


    }
}

