package mq.util

import akka.actor.{ExtensionKey, Extension, ExtendedActorSystem}

/**
 * Created by Bruce on 3/10/15.
 */
// Extension to get the remote address of current node
class RemoteAddressExtension(system: ExtendedActorSystem) extends Extension {
  def address = system.provider.getDefaultAddress
}
object RemoteAddressExtension extends ExtensionKey[RemoteAddressExtension]
