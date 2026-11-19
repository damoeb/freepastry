package rice.p2p.glacier.v2;

import java.lang.reflect.Proxy;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.glacier.FragmentKey;
import rice.p2p.glacier.VersionKey;
import rice.p2p.glacier.v2.messaging.GlacierDataMessage;
import rice.pastry.commonapi.PastryIdFactory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlacierRequestHandlerTest {
  private static final int NUM_FRAGMENTS = 4;
  private static final Logger LOGGER = LoggerFactory.getLogger(GlacierRequestHandlerTest.class);

  private Environment env;
  private PastryIdFactory factory;
  private GlacierTrafficShaper trafficShaper;
  private GlacierRequestHandler testObj;
  private boolean signatureValid;
  private NodeHandle sender;
  private FragmentKey key;
  private Manifest manifest;

  @BeforeEach
  void setUp() {
    env = Environment.directEnvironment(1);
    factory = new PastryIdFactory(env);
    GlacierConfig config = GlacierConfig.fromParameters(env.getParameters());
    Id localId = factory.buildId("local");
    NodeHandle local = GlacierFakes.nodeHandle(localId);
    sender = GlacierFakes.nodeHandle(factory.buildId("sender"));
    List<Object> routed = GlacierFakes.messages();
    GlacierRouter router = GlacierFakes.router(local, routed, env);
    FragmentLocator locator = new FragmentLocator(factory, NUM_FRAGMENTS);

    // no neighbors: this node is responsible for the whole ring
    GlacierNeighborhood neighborhood = new GlacierNeighborhood(
      GlacierFakes.storage((proxy, method, args) -> method.getName().equals("scan") ? factory.buildIdSet() : null),
      factory, localId, config.neighborTimeout(), env, LOGGER);
    neighborhood.determineResponsibleRange();

    GlacierPolicy policy = (GlacierPolicy) Proxy.newProxyInstance(GlacierPolicy.class.getClassLoader(), new Class<?>[] {GlacierPolicy.class}, (proxy, method, args) -> {
      if (method.getName().equals("checkSignature")) {
        return signatureValid;
      }
      return null;
    });

    trafficShaper = new GlacierTrafficShaper(config, NUM_FRAGMENTS, env, LOGGER);
    GlacierRetriever retriever = new GlacierRetriever(config, NUM_FRAGMENTS, 2, "test", policy, null, null, router, locator, trafficShaper, null, env, LOGGER);

    // the fragment is not in the local store, so a manifest-only message makes the node fetch it
    var fragmentStorage = GlacierFakes.storage((proxy, method, args) -> method.getName().equals("exists") ? Boolean.FALSE : null);
    testObj = new GlacierRequestHandler(NUM_FRAGMENTS, 2, "test", policy, router, locator, neighborhood, retriever, fragmentStorage, env, LOGGER);

    key = new FragmentKey(new VersionKey(factory.buildId("object"), 1), 0);
    manifest = new Manifest(new byte[0], new byte[NUM_FRAGMENTS][0], env.getTimeSource().currentTimeMillis() + 1000000, new byte[0], new byte[0]);
  }

  @AfterEach
  void tearDown() {
    env.destroy();
  }

  private void receiveManifestOnly() {
    testObj.handleData(new GlacierDataMessage(1, key, null, manifest, sender, factory.buildId("local"), false, GlacierTags.SYNC));
  }

  @Test
  void aManifestWithAValidSignatureStartsTheFetchOfTheSyncedFragment() {
    signatureValid = true;

    receiveManifestOnly();

    assertTrue(trafficShaper.isPending(key));
  }

  @Test
  void aManifestWithAnInvalidSignatureDoesNotStartTheFetchOfTheSyncedFragment() {
    signatureValid = false;

    receiveManifestOnly();

    assertFalse(trafficShaper.isPending(key), "a fetch was queued for a manifest that is not signed properly");
  }
}
