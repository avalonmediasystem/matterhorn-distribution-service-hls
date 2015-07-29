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
import org.osgi.service.component.ComponentContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Updates the passed playlist file to make all the files relative and returns
     * a List of File objects that includes the playlist itseslf and all of the
     * segments referenced in the playlist.
     */
  public static List<File> relitiviseAndMovePlaylist(File m3u8, File destination) throws IOException, EncoderException {
      List<File> files = new ArrayList<File>();
      files.add(m3u8);

      if (!destination.getParentFile().exists()) {
        destination.getParentFile().mkdirs();
      }

      PrintWriter pw = null;
      BufferedReader br = new BufferedReader(new FileReader(m3u8));
      try {
          pw = new PrintWriter(new FileWriter(destination));
          String line = null;

          //Read from the original file and write to the new
          //unless content matches data to be removed.
          final String oldName = m3u8.getName().replace(".m3u8", "");
          final String newName = destination.getName().replace(".m3u8", "");
          while ((line = br.readLine()) != null) {
              if (line.startsWith(m3u8.getParentFile().getPath())) {
                  File oldFile = new File(line);
                  File newFile = new File(destination.getParentFile(), oldFile.getName().replace(oldName, newName));
                  if (!oldFile.renameTo(newFile)) {
                      throw new EncoderException("Could not rename segment file!");
                  }
                  pw.println(newFile.getName());
              } else {
                  pw.println(line);
              }
          }
      } finally {
          if (pw != null) pw.close();
          br.close();
      }

      //Delete the original file
      if (!m3u8.delete())
         throw new EncoderException("Could not delete origin m3u8 file");

      return files;
    }

  /**
   * Because this implementation technically has several output files, this
   * method shouldn't be used, and the superclass implementation won't work.
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

