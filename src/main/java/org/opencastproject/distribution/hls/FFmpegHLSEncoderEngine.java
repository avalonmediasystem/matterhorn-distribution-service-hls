/**
 *  Copyright 2009, 2010 The Regents of the University of California
 *  Licensed under the Educational Community License, Version 2.0
 *  (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.osedu.org/licenses/ECL-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS"
 *  BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 */

package org.opencastproject.distribution.hls;

import org.opencastproject.composer.api.EncoderException;
import org.opencastproject.composer.api.EncodingProfile;
import org.opencastproject.composer.impl.ffmpeg.FFmpegEncoderEngine;

import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation for the encoder engine backed by ffmpeg.
 */
public class FFmpegHLSEncoderEngine extends FFmpegEncoderEngine {

  /**
   * Creates the ffmpeg encoder engine.
   */
  public FFmpegHLSEncoderEngine() {
    super();
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.opencastproject.composer.impl.FFmpegEncoderEngine#getOutputFile(java.io.File,
   *      org.opencastproject.composer.api.EncodingProfile)
   */
  @Override
  protected File getOutputFile(File source, EncodingProfile profile) {
    File outputFile = null;
    try {
      List<String> arguments = buildArgumentList(profile);
      for (String arg : arguments)
        if (arg.endsWith(".m3u8"))
          outputFile = new File(arguments.get(arguments.size() - 1));
    } catch (EncoderException e) {
      // Unlikely. We checked that before
    }
    return outputFile;
  }
}

