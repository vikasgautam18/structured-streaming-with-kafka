package com.wordpress.technicado

/** Example record
  * {"timestamp":1532806841194,
  * "system":"BADGE",
  * "actor":"bob",
  * "action":"ENTER",
  * "objects":["Building 1"],
  * "location":"45.5,44.3",
  * "message":"Entered Building 1"}
  */

case class UserEvent(
    timestamp: Long,
    system: String,
    actor: String,
    action: String,
    objects: Array[String],
    location: String,
    message: String)