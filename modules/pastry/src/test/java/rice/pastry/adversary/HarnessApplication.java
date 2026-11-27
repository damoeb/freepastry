package rice.pastry.adversary;

import java.io.IOException;

import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.rawserialization.OutputBuffer;
import rice.p2p.commonapi.rawserialization.RawMessage;

/**
 * The workload application on every node: a lookup is a request routed to the key; the node it is delivered to
 * records the delivery and answers the requester directly with its own id, the root it claims. A malicious node that
 * drops does not deliver or answer; a misrouting one answers as if it were the root.
 */
final class HarnessApplication implements Application {

  static final String INSTANCE = "adversary";

  /** What the application reports to the harness. */
  interface Listener {
    /** A lookup request reached {@code member}; returns whether it answers. */
    boolean lookupArrived(long lookupId, Oracle.Member member);

    void lookupAnswered(long lookupId, rice.pastry.Id claimedRoot);
  }

  private final Endpoint endpoint;
  private final Oracle.Member member;
  private final Listener listener;

  HarnessApplication(Oracle.Member member, Listener listener) {
    this.member = member;
    this.listener = listener;
    endpoint = member.node.buildEndpoint(this, INSTANCE);
    endpoint.setDeserializer((buf, type, priority, sender) -> {
      // the direct simulator passes message objects, it never deserializes
      throw new IOException("unexpected message type " + type);
    });
    endpoint.register();
  }

  void lookup(long lookupId, rice.pastry.Id key) {
    endpoint.route(key, new LookupRequest(lookupId, endpoint.getLocalNodeHandle()), null);
  }

  @Override
  public void deliver(Id id, Message message) {
    if (message instanceof LookupRequest request) {
      if (listener.lookupArrived(request.lookupId, member)) {
        endpoint.route(null, new LookupReply(request.lookupId, member.id), request.origin);
      }
    } else if (message instanceof LookupReply reply) {
      listener.lookupAnswered(reply.lookupId, reply.claimedRoot);
    }
  }

  @Override
  public boolean forward(RouteMessage message) {
    return true;
  }

  @Override
  public void update(NodeHandle handle, boolean joined) {}

  /** A lookup, routed to its key. */
  static final class LookupRequest implements RawMessage {
    static final short TYPE = 1;
    final long lookupId;
    final NodeHandle origin;

    LookupRequest(long lookupId, NodeHandle origin) {
      this.lookupId = lookupId;
      this.origin = origin;
    }

    @Override
    public int getPriority() {
      return Message.MEDIUM_PRIORITY;
    }

    @Override
    public short getType() {
      return TYPE;
    }

    /** Only for the byte estimate: the id stands in for the origin's handle. */
    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeLong(lookupId);
      origin.getId().serialize(buf);
    }
  }

  /** The answer to a lookup, sent directly to the requester. */
  static final class LookupReply implements RawMessage {
    static final short TYPE = 2;
    final long lookupId;
    final rice.pastry.Id claimedRoot;

    LookupReply(long lookupId, rice.pastry.Id claimedRoot) {
      this.lookupId = lookupId;
      this.claimedRoot = claimedRoot;
    }

    @Override
    public int getPriority() {
      return Message.MEDIUM_PRIORITY;
    }

    @Override
    public short getType() {
      return TYPE;
    }

    @Override
    public void serialize(OutputBuffer buf) throws IOException {
      buf.writeLong(lookupId);
      claimedRoot.serialize(buf);
    }
  }
}
