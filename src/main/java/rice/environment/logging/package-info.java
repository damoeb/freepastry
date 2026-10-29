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
 * Logging. FreePastry logs through the SLF4J 2 API ({@link org.slf4j.Logger}). A
 * {@link rice.environment.logging.LogManager}, available from {@code environment.getLogManager()}, hands out one
 * logger per class and instance name. The default {@link rice.environment.logging.Slf4jLogManager} names it
 * {@code <class>[.<instance>][.<prefix>]} and creates it with {@link org.slf4j.LoggerFactory}; node factories call
 * {@link rice.environment.logging.Slf4jLogManager#clone(String)} with the node id, so the loggers of different nodes
 * in one JVM can be told apart. Levels, format and output are configured in the SLF4J backend that the application
 * puts on the classpath, not in FreePastry's parameters.
 */
package rice.environment.logging;
