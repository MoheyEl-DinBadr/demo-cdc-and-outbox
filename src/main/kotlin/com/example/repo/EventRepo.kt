package com.example.repo

import com.example.model.Event
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Sorts
import io.quarkus.mongodb.panache.reactive.ReactivePanacheMongoRepository
import io.smallrye.mutiny.Multi
import jakarta.enterprise.context.ApplicationScoped
import org.bson.Document

@ApplicationScoped
class EventRepo: ReactivePanacheMongoRepository<Event> {

    fun findEarliestNonProcessed(): Multi<Event> {
        val filter:Document = Document.parse(eq("processed", false).toBsonDocument().toJson());
        return this.find(filter, Document.parse(Sorts.ascending("soeTime").toBsonDocument().toJson()))
                .stream()
    }
}
