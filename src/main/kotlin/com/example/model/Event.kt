package com.example.model

import io.quarkus.mongodb.panache.common.MongoEntity
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.types.ObjectId
import java.time.LocalDateTime

@MongoEntity(collection = "events")
class Event(){
    @BsonId lateinit var id:ObjectId
    lateinit var event:String
    lateinit var soeTime:LocalDateTime
    lateinit var topic: String
    var processed:Boolean = false
    constructor(id:ObjectId, event: String, soeTime:LocalDateTime, topic:String, processed:Boolean = false) : this() {
        this.id = id
        this.event = event
        this.soeTime = soeTime
        this.topic = topic
        this.processed = processed
    }
}
