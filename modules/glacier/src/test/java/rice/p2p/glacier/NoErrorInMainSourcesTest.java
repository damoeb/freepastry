package rice.p2p.glacier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * An {@link Error} thrown on the selector thread reaches the environment's exception strategy, which by default
 * destroys the environment and every node in it; the transport survives a {@link RuntimeException} from a message
 * handler, but not an {@code Error}. Code that handles what peers send therefore never throws one: input from the
 * wire is dropped and logged, and a broken local invariant throws an {@link IllegalStateException}.
 *
 * <p>A plain source scan rather than an architecture test, so it needs no extra test dependency.
 */
@DisplayName("main sources")
class NoErrorInMainSourcesTest {
  private static final Pattern NEW_ERROR = Pattern.compile("\\bnew\\s+(java\\.lang\\.)?Error\\s*\\(");

  private static Path modulesDirectory() {
    Path directory = Path.of("").toAbsolutePath();
    while (directory != null && !Files.isDirectory(directory.resolve("modules"))) {
      directory = directory.getParent();
    }
    assertNotNull(directory, "the build runs inside the repository");
    return directory.resolve("modules");
  }

  @Test
  @DisplayName("no module throws a new java.lang.Error")
  void noNewError() throws IOException {
    List<Path> sourceRoots;
    try (Stream<Path> modules = Files.list(modulesDirectory())) {
      sourceRoots = modules.map(module -> module.resolve("src/main/java")).filter(Files::isDirectory).toList();
    }
    assertTrue(sourceRoots.size() > 1, () -> "found the main source roots: " + sourceRoots);

    List<String> found = new ArrayList<>();
    for (Path root : sourceRoots) {
      try (Stream<Path> files = Files.walk(root)) {
        for (Path file : files.filter(f -> f.toString().endsWith(".java")).toList()) {
          List<String> lines = Files.readAllLines(file);
          for (int i = 0; i < lines.size(); i++) {
            if (NEW_ERROR.matcher(lines.get(i)).find()) {
              found.add(modulesDirectory().relativize(file) + ":" + (i + 1) + ": " + lines.get(i).strip());
            }
          }
        }
      }
    }

    assertEquals(List.of(), found);
  }
}
