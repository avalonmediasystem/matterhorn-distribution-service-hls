/*
 * This file has been modified from the original distribution. 
 * Modifications Copyright 2013 The Trustees of Indiana University and Northwestern University.
 */

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
import org.opencastproject.util.data.Option;

import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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
 
  public void activate(ComponentContext cc) {
    super.activate(cc);
  }
 
  public Option<File> encode(File mediaSource, EncodingProfile format, Map<String, String> properties) throws EncoderException {
    super.encode(mediaSource, format, properties);
    File m3u8 = getOutputFile(mediaSource, format); 
    File tempFile = null;
 
    // Modify the m3u8 file to contain file names instead of absolute paths to such files
    try {

      //Construct the new file that will later be renamed to the original filename. 
      tempFile = new File(m3u8.getAbsolutePath() + ".tmp");
      
      BufferedReader br = new BufferedReader(new FileReader(m3u8));
      PrintWriter pw = new PrintWriter(new FileWriter(tempFile));
      
      String line = null;

      //Read from the original file and write to the new 
      //unless content matches data to be removed.
      while ((line = br.readLine()) != null) {
        String pattern = "\\/.*\\/";
        pw.println(line.replaceAll(pattern, ""));
        pw.flush();
      }
      pw.close();
      br.close();
      
      //Delete the original file
      if (!m3u8.delete())
        throw new EncoderException("Could not delete origin m3u8 file");
      
      //Rename the new file to the filename the original file had.
      if (!tempFile.renameTo(m3u8))
        throw new EncoderException("Could not rename m3u8 tempfile");
    }
    catch (FileNotFoundException ex) {
      ex.printStackTrace();
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }

    return Option.some(tempFile);
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
        if (arg.endsWith(".m3u8")) {
          outputFile = new File(arg);
        }
    } catch (EncoderException e) {
      // Unlikely. We checked that before
    }
    return outputFile;
  }
}

