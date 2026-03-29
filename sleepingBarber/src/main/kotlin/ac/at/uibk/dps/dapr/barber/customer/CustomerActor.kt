package ac.at.uibk.dps.dapr.barber.customer

import io.dapr.actors.ActorMethod
import io.dapr.actors.ActorType

@ActorType(name = "CustomerActor")
interface CustomerActor {
  @ActorMethod(name = "request") fun request()

  @ActorMethod(name = "full") fun full(data: Map<String, Any>)

  @ActorMethod(name = "comeIn") fun comeIn(data: Map<String, Any>)

  @ActorMethod(name = "done") fun done(data: Map<String, Any>)
}
