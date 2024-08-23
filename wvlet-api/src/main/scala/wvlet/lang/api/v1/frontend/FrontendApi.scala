package wvlet.lang.api.v1.frontend

import wvlet.airframe.http.{RPC, RxRouter, RxRouterProvider}

@RPC
class FrontendApi:
  def status: String = "ok"

object FrontendApi extends RxRouterProvider:
  override def router = RxRouter.of[FrontendApi]
