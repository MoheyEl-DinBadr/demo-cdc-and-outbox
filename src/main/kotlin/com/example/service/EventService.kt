package com.example.service

import com.example.model.Event
import com.example.repo.EventRepo
import com.github.javafaker.Faker
import io.quarkus.scheduler.Scheduled
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import io.smallrye.reactive.messaging.kafka.KafkaClientService
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata.OutgoingKafkaRecordMetadataBuilder
import jakarta.enterprise.context.ApplicationScoped
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.bson.types.ObjectId
import org.eclipse.microprofile.reactive.messaging.Message
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.util.stream.Collectors
import java.util.stream.IntStream

@ApplicationScoped
class EventService(val repo: EventRepo, val kafkaClient: KafkaClientService) {

    fun findEarliestNonProcessed(): Multi<Event> = repo.findEarliestNonProcessed()
    fun update(events: List<Event>): Uni<Void> = repo.update(events)
    fun update(event: Event): Uni<Event> = repo.update(event)

    fun updateEventStatus(event: Event): Event {
        event.processed = true
        return event
    }
    fun updateDocument(id:ObjectId, event: String, topic:String, soeTime:LocalDateTime, processed:Boolean): Uni<Event>  {

        val eventObj:Event = Event(id, event, soeTime, topic, processed)
        return repo.persistOrUpdate(eventObj)
    }


    fun publishEvent(event: Event): Uni<Event> {

        val eventData = event.event
        val soeTime = Timestamp.valueOf(event.soeTime).toInstant()
        val topic = event.topic
        val id = event.id.toHexString()
        return publishEvent(eventData, topic, id, soeTime).replaceWith(event)
    }

    fun publishEvent(event: String, topic:String, id:String, soeTime:Instant): Uni<RecordMetadata> {
        val producer = kafkaClient.getProducer<String, Message<String>>("producer")
        val record = ProducerRecord<String, Message<String>>(topic, Message.of(event)
                .addMetadata(OutgoingKafkaRecordMetadataBuilder<String>().withTopic(topic)
                        .withKey(id)
                        .withTimestamp(soeTime)
                        .build())
        )
        return producer.send(record)
    }

    @Scheduled(every = "1m")
    fun generateData() {
            val faker = Faker.instance()
        val list = IntStream.range(0, 10)
                .boxed()
                .map { Event(ObjectId.get(), faker.address().firstName(), LocalDateTime.now(), "topic${it}") }
                .collect(Collectors.toList())
        repo.persist(list)
                .await()
                .indefinitely()
    }
}
