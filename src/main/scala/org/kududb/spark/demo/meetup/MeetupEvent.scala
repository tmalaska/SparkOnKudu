package org.kududb.spark.demo.meetup

import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper

class MeetupEvent(var eventId:String = "",
                  var memberId:Int = 0,
                  var rsvpId:Int = 0,
                  var eventName:String = "",
                  var eventURL:String = "",
                  var time:Long = 0,
                  var guests:Int = 0,
                  var memberName:String = "",
                  var facebookIdentifier:String = "",
                  var linkedinIdentifier:String = "",
                  var twitterIdentifier:String = "",
                  var photo:String = "", 
                  var mtime:Int = 0,
                  var response:String = "",
                  var latitude:Double = 0.0,
                  var longitude:Double = 0.0,
                  var venueId:Int = 0,
                  var venueName:String = "",
                  var visibility:String = "",
                  var isInsert:Boolean = false,
                  var hasChanged:Boolean = false) extends Serializable {

  override def toString():String = {
      new ObjectMapper().writeValueAsString(this);
  }

}

//object MeetupEventBuilder extends Serializable  {
//  def build(input:String):MeetupEvent = {
//    val mapper = new ObjectMapper with ScalaObjectMapper
//        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//        mapper.registerModule(DefaultScalaModule)
//        mapper.readValue(input, classOf[MeetupEvent])
//  }
//}
