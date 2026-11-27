/*******************************************************************************

"FreePastry" Peer-to-Peer Application Development Substrate

Copyright 2002-2007, Rice University. Copyright 2006-2007, Max Planck Institute
for Software Systems.  All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

- Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

- Neither the name of Rice  University (RICE), Max Planck Institute for Software
Systems (MPI-SWS) nor the names of its contributors may be used to endorse or
promote products derived from this software without specific prior written
permission.

This software is provided by RICE, MPI-SWS and the contributors on an "as is"
basis, without any representations or warranties of any kind, express or implied
including, but not limited to, representations or warranties of
non-infringement, merchantability or fitness for a particular purpose. In no
event shall RICE, MPI-SWS or contributors be liable for any direct, indirect,
incidental, special, exemplary, or consequential damages (including, but not
limited to, procurement of substitute goods or services; loss of use, data, or
profits; or business interruption) however caused and on any theory of
liability, whether in contract, strict liability, or tort (including negligence
or otherwise) arising in any way out of the use of this software, even if
advised of the possibility of such damage.

*******************************************************************************/
package rice.p2p.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Files and directories that only their owner may read: what a node stores (objects, metadata, its parameters) may be
 * private to the node's owner, so a new file is created {@code rw-------} and a new directory {@code rwx------}
 * (POSIX), whatever the umask, and never readable by others, not even briefly. A file or directory that exists already
 * keeps its permissions. Where the file system has no POSIX permissions (e.g. Windows) the platform defaults apply.
 */
public final class PrivateFiles {
  private static final String OWNER_ONLY_FILE = "rw-------";
  private static final String OWNER_ONLY_DIRECTORY = "rwx------";

  private PrivateFiles() {
  }

  /** @return whether the default file system has POSIX permissions */
  public static boolean isPosix() {
    return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
  }

  /**
   * Like {@link Files#newOutputStream(Path, OpenOption...)}, but a file it creates is readable by its owner only.
   *
   * @param file the file
   * @param options as for {@link Files#newOutputStream}; none means CREATE, TRUNCATE_EXISTING, WRITE
   * @return the stream
   * @throws IOException if the file cannot be opened
   */
  public static OutputStream newOutputStream(Path file, OpenOption... options) throws IOException {
    Set<OpenOption> openOptions = new HashSet<>(options.length == 0
        ? List.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        : List.of(options));
    openOptions.add(StandardOpenOption.WRITE);
    return Channels.newOutputStream(Files.newByteChannel(file, openOptions, ownerOnly(OWNER_ONLY_FILE)));
  }

  /**
   * Like {@link Files#createDirectories(Path, FileAttribute...)}, but the directories it creates are readable by their
   * owner only.
   *
   * @param directory the directory
   * @return the directory
   * @throws IOException if it cannot be created
   */
  public static Path createDirectories(Path directory) throws IOException {
    return Files.createDirectories(directory, ownerOnly(OWNER_ONLY_DIRECTORY));
  }

  private static FileAttribute<?>[] ownerOnly(String permissions) {
    if (!isPosix()) return new FileAttribute<?>[0];
    Set<PosixFilePermission> set = PosixFilePermissions.fromString(permissions);
    return new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(set)};
  }
}
