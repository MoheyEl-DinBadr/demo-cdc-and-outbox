package com.example.service

import io.quarkus.arc.properties.IfBuildProperty
import io.quarkus.runtime.Startup
import io.quarkus.scheduler.Scheduled
import jakarta.enterprise.context.ApplicationScoped

@Startup
@ApplicationScoped
@IfBuildProperty(name = "enable.outbox", stringValue = "true")
class OutBoxService(val eventService: EventService) {

    @Scheduled(every = "1m")
    fun processEvents() {
        eventService.findEarliestNonProcessed()
            .onItem().transformToUni { eventService.publishEvent(it) }
            .merge()
            .map { it.processed = true; it }
            .collect()
            .asList()
            .flatMap { eventService.update(it) }
            .await().indefinitely()
    }
}
