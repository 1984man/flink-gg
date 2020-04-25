package com.biubiu.druid

object model {
  //{"news_entry_id":"e1","country":"ng","language":"en","impression":10,"click":100}
  case class SimpleEvent(timestamp:Long,news_entry_id:String,country:String,language:String,impression:Long,click:Long)
}
