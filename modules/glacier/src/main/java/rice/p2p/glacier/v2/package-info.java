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
/**
 * Glacier, durable archival storage. {@link rice.p2p.glacier.v2.GlacierImpl} encodes each object into
 * {@code numFragments} erasure-coded fragments, any {@code numSurvivors} of which suffice to restore it,
 * and stores them, together with a signed {@link rice.p2p.glacier.v2.Manifest}, on nodes chosen from
 * the object key and fragment index. Nodes periodically synchronize their fragments with their
 * neighbors using bloom filters, hand off fragments they no longer own, and recover missing ones.
 * <p>
 * Threading: erasure coding and bloom filter construction run on the processing thread
 * ({@code Endpoint.process()}); everything else runs on the selector thread.
 */
package rice.p2p.glacier.v2;
