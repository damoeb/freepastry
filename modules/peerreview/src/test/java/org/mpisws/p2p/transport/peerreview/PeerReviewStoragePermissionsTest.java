package org.mpisws.p2p.transport.peerreview;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.EOFException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mpisws.p2p.transport.peerreview.commitment.AuthenticatorStore;
import org.mpisws.p2p.transport.peerreview.history.SecureHistoryFactory;
import org.mpisws.p2p.transport.peerreview.history.hasher.SHA256HashProvider;
import org.mpisws.p2p.transport.peerreview.infostore.PeerInfoStoreImpl;
import org.mpisws.p2p.transport.util.Serializer;
import rice.environment.Environment;
import rice.p2p.util.PrivateFiles;

/** What PeerReview stores is private to the node's owner: new files are rw-------, new directories rwx------. */
class PeerReviewStoragePermissionsTest {
  private final Environment environment = new Environment();

  @TempDir
  Path directory;

  @BeforeEach
  void requirePosix() {
    assumeTrue(PrivateFiles.isPosix(), "needs POSIX file permissions");
  }

  @AfterEach
  void tearDown() {
    environment.destroy();
  }

  private static String permissionsOf(Path path) throws Exception {
    return PosixFilePermissions.toString(Files.getPosixFilePermissions(path));
  }

  @Test
  void aNewHistoryIsOwnerOnly() throws Exception {
    String name = directory.resolve("history").toString();

    new SecureHistoryFactory(new SHA256HashProvider(), environment).create(name, 0, new byte[32]).close();

    assertEquals("rw-------", permissionsOf(Path.of(name + ".index")));
    assertEquals("rw-------", permissionsOf(Path.of(name + ".data")));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void aNewAuthenticatorFileIsOwnerOnly() throws Exception {
    // the store reads the file until a deserializer throws: the end of the empty file
    Serializer endOfFile = (Serializer) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {Serializer.class}, (proxy, method, args) -> {
          throw new EOFException();
        });
    PeerReview peerReview = (PeerReview) Proxy.newProxyInstance(
        getClass().getClassLoader(), new Class<?>[] {PeerReview.class},
        (proxy, method, args) -> method.getName().equals("getIdSerializer") ? endOfFile : null);
    AuthenticatorStore store = new AuthenticatorStore(peerReview);
    Path file = directory.resolve("authenticators");

    store.setFilename(file);

    assertEquals("rw-------", permissionsOf(file));
  }

  @Test
  void aNewInfoStoreDirectoryIsOwnerOnly() throws Exception {
    PeerInfoStoreImpl<?, ?> store = new PeerInfoStoreImpl<>(null, null, null, null, environment);
    Path storage = directory.resolve("info").resolve("store");

    store.setStorageDirectory(storage);

    assertEquals("rwx------", permissionsOf(storage));
    assertEquals("rwx------", permissionsOf(storage.getParent()));
  }

  @Test
  void aDirectoryThatExistsKeepsItsPermissions() throws Exception {
    PeerInfoStoreImpl<?, ?> store = new PeerInfoStoreImpl<>(null, null, null, null, environment);
    Path storage = Files.createDirectory(directory.resolve("existing"));
    Files.setPosixFilePermissions(storage, PosixFilePermissions.fromString("rwxr-xr-x"));

    store.setStorageDirectory(storage);

    assertEquals("rwxr-xr-x", permissionsOf(storage));
  }
}
