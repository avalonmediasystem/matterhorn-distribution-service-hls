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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.opencastproject.distribution.api.DistributionException;
import org.opencastproject.distribution.api.DistributionService;
//import org.opencastproject.distribution.api.DownloadDistributionService;
import org.opencastproject.job.api.AbstractJobProducer;
import org.opencastproject.composer.api.EncodingProfile;
import org.opencastproject.job.api.Job;
import org.opencastproject.mediapackage.MediaPackage;
import org.opencastproject.mediapackage.MediaPackageElement;
import org.opencastproject.mediapackage.MediaPackageElementParser;
import org.opencastproject.mediapackage.MediaPackageException;
import org.opencastproject.mediapackage.MediaPackageParser;
import org.opencastproject.mediapackage.track.TrackImpl;
import org.opencastproject.security.api.OrganizationDirectoryService;
import org.opencastproject.security.api.SecurityService;
import org.opencastproject.security.api.TrustedHttpClient;
import org.opencastproject.security.api.UserDirectoryService;
import org.opencastproject.serviceregistry.api.ServiceRegistry;
import org.opencastproject.serviceregistry.api.ServiceRegistryException;
import org.opencastproject.util.FileSupport;
import org.opencastproject.util.MimeType;
import org.opencastproject.util.NotFoundException;
import org.opencastproject.util.PathSupport;
import org.opencastproject.util.UrlSupport;
import org.opencastproject.workspace.api.Workspace;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;;

/**
 * Distributes media to the local media delivery directory.
 */
public class HLSDistributionServiceImpl extends AbstractJobProducer implements DistributionService/*, DownloadDistributionService*/ {

  /** Logging facility */
  private static final Logger logger = LoggerFactory.getLogger(HLSDistributionServiceImpl.class);

  /** List of available operations on jobs */
  private enum Operation {
    Distribute, Retract
  }

  /** Receipt type */
  public static final String JOB_TYPE = "org.opencastproject.distribution.hls";

  /** Default distribution directory */
  public static final String DEFAULT_DISTRIBUTION_DIR = "opencast" + File.separator + "static";

  /** Timeout in millis for checking distributed file request */
  private static final long TIMEOUT = 10000L;

  /** Interval time in millis for checking distributed file request */
  private static final long INTERVAL = 300L;

  /** Path to the distribution directory */
  protected File distributionDirectory = null;

  /** this media hls service's base URL */
  protected String serviceUrl = null;

  /** The remote service registry */
  protected ServiceRegistry serviceRegistry = null;

  /** The workspace reference */
  protected Workspace workspace = null;

  /** The security service */
  protected SecurityService securityService = null;

  /** The user directory service */
  protected UserDirectoryService userDirectoryService = null;

  /** The organization directory service */
  protected OrganizationDirectoryService organizationDirectoryService = null;

  /** The trusted HTTP client */
  private TrustedHttpClient trustedHttpClient;

  /** the encoding engine */
  private FFmpegHLSEncoderEngine engine;

  /**
   * Creates a new instance of the hls distribution service.
   */
  public HLSDistributionServiceImpl() {
    super(JOB_TYPE);
  }

  /**
   * Activate method for this OSGi service implementation.
   * 
   * @param cc
   *          the OSGi component context
   */
  protected void activate(ComponentContext cc) {
    engine = new FFmpegHLSEncoderEngine();
    engine.activate(cc);

    serviceUrl = cc.getBundleContext().getProperty("org.opencastproject.hls.url");
    if (serviceUrl == null)
      throw new IllegalStateException("HLS url must be set (org.opencastproject.hls.url)");

    String ccDistributionDirectory = cc.getBundleContext().getProperty("org.opencastproject.hls.directory");
    if (ccDistributionDirectory == null)
      throw new IllegalStateException("Distribution directory must be set (org.opencastproject.hls.directory)");
    this.distributionDirectory = new File(ccDistributionDirectory);
    logger.info("HLS distribution directory is {}", distributionDirectory);
  }

  @Override
  public Job distribute(MediaPackage mediapackage, String elementId) throws DistributionException, MediaPackageException {
    return distribute(mediapackage, elementId, true);
  }

  public Job distribute(MediaPackage mediapackage, String elementId, boolean checkAvailability)
          throws DistributionException, MediaPackageException {
    if (mediapackage == null)
      throw new MediaPackageException("Mediapackage must be specified");
    if (elementId == null)
      throw new MediaPackageException("Element ID must be specified");
    try {
      return serviceRegistry.createJob(JOB_TYPE,
                                       Operation.Distribute.toString(),
                                       Arrays.asList(MediaPackageParser.getAsXml(mediapackage),
                                                     elementId,
                                                     Boolean.toString(checkAvailability)));
    } catch (ServiceRegistryException e) {
      throw new DistributionException("Unable to create a job", e);
    }
  }

  /**
   * Distributes the mediapackage's element to the location that is returned by the concrete implementation. In
   * addition, a representation of the distributed element is added to the mediapackage.
   *
   * @see org.opencastproject.distribution.api.DistributionService#distribute(org.opencastproject.mediapackage.MediaPackage,
   *      String)
   * @throws org.opencastproject.distribution.api.DistributionException
   *           in case of an error
   */
  protected MediaPackageElement distribute(Job job, MediaPackage mediapackage, String elementId, boolean checkAvailability)
          throws DistributionException {
    return distributeElement(mediapackage, elementId, checkAvailability);
  }

  /**
   * Distribute a Mediapackage element to the hls distribution service.
   * 
   * @param mediapackage
   *          The media package that contains the element to distribute.
   * @param elementId
   *          The id of the element that should be distributed contained within the media package.
   * @param checkAvailability
   *          Check the availability of the distributed element via http.
   * @return A reference to the MediaPackageElement that has been distributed.
   * @throws DistributionException
   *           Thrown if the parent directory of the MediaPackageElement cannot be created, if the MediaPackageElement
   *           cannot be copied or another unexpected exception occurs.
   */
  public MediaPackageElement distributeElement(MediaPackage mediapackage, String elementId, boolean checkAvailability)
          throws DistributionException {
    if (mediapackage == null)
      throw new IllegalArgumentException("Mediapackage must be specified");
    if (elementId == null)
      throw new IllegalArgumentException("Element ID must be specified");

    String mediaPackageId = mediapackage.getIdentifier().compact();
    MediaPackageElement element = mediapackage.getElementById(elementId);

    // Make sure the element exists
    if (mediapackage.getElementById(elementId) == null)
      throw new IllegalStateException("No element " + elementId + " found in mediapackage");
    if (!accepts(element)) {
      logger.debug("Skipping {} {} for distribution to the streaming server", element.getElementType().toString()
        .toLowerCase(), element.getIdentifier());
      return null;
    }

    try {
      File source;
      try {
        source = workspace.get(element.getURI());
      } catch (NotFoundException e) {
        throw new DistributionException("Unable to find " + element.getURI() + " in the workspace", e);
      } catch (IOException e) {
        throw new DistributionException("Error loading " + element.getURI() + " from the workspace", e);
      }
      File destination = getDistributionFile(mediapackage, element);

      // Make the directories to place the files into
      try {
        FileUtils.forceMkdir(destination.getParentFile());
      } catch (IOException e) {
        throw new DistributionException("Unable to create " + destination.getParentFile(), e);
      }
      logger.debug("Distributing {} to {}", elementId, destination);

      // Do the HLS segmentation and m3u8 playlist generation
      final Map<String, String> properties = new HashMap<String, String>();
      properties.put("profile.hls.http.name", "hls");
      properties.put("profile.hls.http.input", "visual");
      properties.put("profile.hls.http.output", "visual");
      properties.put("profile.hls.http.suffix", ".m3u8");
      properties.put("profile.hls.http.mimetype", "application/x-mpegURL");
      properties.put("profile.hls.http.ffmpeg.command", "-i #{in.video.path} -codec copy -map 0 -bsf h264_mp4toannexb -f segment -segment_list #{outputdir}/#{outputname}#{out.suffix} -segment_time 10 #{outputdir}/#{outputname}-%03d.ts");
      EncodingProfile profile = createEncodingProfile("profile.hls.http", ".m3u8", properties);
      
      final Map<String, String> commandLineOpts = new HashMap<String, String>();
      commandLineOpts.put("outputdir", destination.getAbsoluteFile().getParent());
      commandLineOpts.put("outputname", FilenameUtils.getBaseName(destination.getName()));

      try {
        engine = engine == null ? new FFmpegHLSEncoderEngine() : engine;
        File playlistFile = engine.encode(source, profile, commandLineOpts).getOrElseNull();
      } catch (Exception e) {
        throw new DistributionException("Unable to generare HLS segments and playlists for " + source + " in " + destination.getAbsoluteFile().getParent(), e);
      }

      // Create a representation of the distributed file in the mediapackage
      MediaPackageElement distributedElement = (MediaPackageElement) element.clone();
      try {
        distributedElement.setURI(getDistributionUri(mediaPackageId, element));
      } catch (URISyntaxException e) {
        throw new DistributionException("Distributed element produces an invalid URI", e);
      }
      distributedElement.setMimeType(MimeType.mimeType("application", "x-mpegURL"));
      distributedElement.setIdentifier(null);

      logger.info("Finished distribution of {}", element);

      return distributedElement;
    } catch (Exception e) {
      logger.warn("Error distributing " + element, e);
      if (e instanceof DistributionException) {
        throw (DistributionException) e;
      } else {
        throw new DistributionException(e);
      }
    }
  }

  /**
   * Creates EncodingProfile.
   * 
   * @param name
   * @param suffix
   * @param properties
   * @return
   */
  private EncodingProfile createEncodingProfile(final String name, final String suffix,
          final Map<String, String> properties) {
    EncodingProfile profile = new EncodingProfile() {
      @Override
      public boolean isApplicableTo(MediaType type) {
        return false;
      }

      @Override
      public boolean hasExtensions() {
        return false;
      }

      @Override
      public String getSuffix() {
        return suffix;
      }

      @Override
      public Object getSource() {
        return null;
      }

      @Override
      public MediaType getOutputType() {
        return null;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getMimeType() {
        return null;
      }

      @Override
      public String getIdentifier() {
        return name;
      }

      @Override
      public Map<String, String> getExtensions() {
        return properties;
      }

      @Override
      public String getExtension(String key) {
        return properties.get(getIdentifier() + "." + key);
      }

      @Override
      public MediaType getApplicableMediaType() {
        return null;
      }
    };
    return profile;
  }

  @Override
  public Job retract(MediaPackage mediaPackage, String elementId) throws DistributionException {
    if (mediaPackage == null)
      throw new IllegalArgumentException("Mediapackage must be specified");
    if (elementId == null)
      throw new IllegalArgumentException("Element ID must be specified");
    try {
      List<String> arguments = new ArrayList<String>();
      arguments.add(MediaPackageParser.getAsXml(mediaPackage));
      arguments.add(elementId);
      return serviceRegistry.createJob(JOB_TYPE, Operation.Retract.toString(), arguments);
    } catch (ServiceRegistryException e) {
      throw new DistributionException("Unable to create a job", e);
    }
  }

  /**
   * Retract a media package element from the distribution channel. The retracted element must not necessarily be the
   * one given as parameter <code>elementId</code>. Instead, the element's distribution URI will be calculated and then
   * in turn be matched against each element of the package. This way you are able to retract elements by providing the
   * "original" element here.
   * 
   * @param job
   *          the associated job
   * @param mediapackage
   *          the mediapackage
   * @param elementId
   *          the element identifier
   * @return the retracted element or <code>null</code> if the element was not retracted
   * @throws org.opencastproject.distribution.api.DistributionException
   *           in case of an error
   */
  protected MediaPackageElement retract(Job job, MediaPackage mediapackage, String elementId)
          throws DistributionException {

    if (mediapackage == null)
      throw new IllegalArgumentException("Mediapackage must be specified");
    if (elementId == null)
      throw new IllegalArgumentException("Element ID must be specified");

    // Make sure the element exists
    MediaPackageElement element = mediapackage.getElementById(elementId);
    if (element == null)
      throw new IllegalStateException("No element " + elementId + " found in mediapackage");

    // Find the element that has been created as part of the distribution process
    String mediapackageId = mediapackage.getIdentifier().compact();
    URI distributedURI = null;
    MediaPackageElement distributedElement = element;
    try {
      File mediapackageDir = getMediaPackageDirectory(mediapackageId);
      File elementDir = getDistributedFile(mediapackage, element);

      // Does the file exist? If not, the current element has not been distributed to this channel
      // or has been removed otherwise
      if (!elementDir.exists()) {
        throw new Exception("Track directory does not exist: " + elementDir.getAbsolutePath());
      }

      // Try to remove the file and - if possible - the parent folder
      System.out.println("Removing track folder: " + elementDir.getAbsolutePath());
      FileUtils.forceDelete(elementDir);
      logger.info("Removed track folder: " + elementDir.getAbsolutePath());

      if (mediapackageDir.list().length == 0) {
        logger.info("Removed parent folder since it is empty: " + mediapackageDir.getAbsolutePath());
        FileSupport.delete(mediapackageDir);
      }

      logger.info("Finished rectracting element {} of media package {}", elementId, mediapackageId);

      return distributedElement;
    } catch (Exception e) {
      logger.warn("Error retracting element " + elementId + " of mediapackage " + mediapackageId, e);
      if (e instanceof DistributionException) {
        throw (DistributionException) e;
      } else {
        throw new DistributionException(e);
      }
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.opencastproject.job.api.AbstractJobProducer#process(org.opencastproject.job.api.Job)
   */
  @Override
  protected String process(Job job) throws Exception {
    Operation op = null;
    String operation = job.getOperation();
    List<String> arguments = job.getArguments();
    try {
      op = Operation.valueOf(operation);
      MediaPackage mediapackage = MediaPackageParser.getFromXml(arguments.get(0));
      String elementId = arguments.get(1);
      switch (op) {
        case Distribute:
          Boolean checkAvailability = Boolean.parseBoolean(arguments.get(2));
          MediaPackageElement distributedElement = distribute(job, mediapackage, elementId, checkAvailability);
          return (distributedElement != null) ? MediaPackageElementParser.getAsXml(distributedElement) : null;
        case Retract:
          MediaPackageElement retractedElement = retract(job, mediapackage, elementId);
          return (retractedElement != null) ? MediaPackageElementParser.getAsXml(retractedElement) : null;
        default:
          throw new IllegalStateException("Don't know how to handle operation '" + operation + "'");
      }
    } catch (IllegalArgumentException e) {
      throw new ServiceRegistryException("This service can't handle operations of type '" + op + "'", e);
    } catch (IndexOutOfBoundsException e) {
      throw new ServiceRegistryException("This argument list for operation '" + op + "' does not meet expectations", e);
    } catch (Exception e) {
      throw new ServiceRegistryException("Error handling operation '" + op + "'", e);
    }
  }

  /**
   * Gets the destination file to copy the contents of a mediapackage element.
   * 
   * @param mediaPackage
   *          the media package
   * @param element
   *          The mediapackage element being distributed
   * @return The file to copy the content to
   */
  protected File getDistributionFile(MediaPackage mediaPackage, MediaPackageElement element) {
    String elementId = element.getIdentifier();
    String fileName = FilenameUtils.getName(element.getURI().toString()) + ".m3u8";
    String directoryName = distributionDirectory.getAbsolutePath();
    String destinationFileName = PathSupport.concat(new String[] { directoryName,
            mediaPackage.getIdentifier().compact(), elementId, fileName });
    return new File(destinationFileName);
  }

  /**
   * Gets the distributed file from mediapackage information.
   * 
   * @param mediaPackage
   *          the media package
   * @param element
   *          The mediapackage element being distributed
   * @return The file to copy the content to
   */
  protected File getDistributedFile(MediaPackage mediaPackage, MediaPackageElement element) {
    String elementUid = element.getReference().getIdentifier();

    String directoryName = distributionDirectory.getAbsolutePath();
    String distributedFileName = PathSupport.concat(new String[] { directoryName,
            mediaPackage.getIdentifier().compact(), elementUid });
    return new File(distributedFileName);
  }

  /**
   * Gets the URI for the element to be distributed.
   * 
   * @param mediaPackageId
   *          the mediapackage identifier
   * @param element
   *          The mediapackage element being distributed
   * @return The resulting URI after distribution
   * @throws URISyntaxException
   *           if the concrete implementation tries to create a malformed uri
   */
  protected URI getDistributionUri(String mediaPackageId, MediaPackageElement element) throws URISyntaxException {
    String elementId = element.getIdentifier();
    String fileName = FilenameUtils.getName(element.getURI().toString()) + ".m3u8";
    String destinationURI = UrlSupport.concat(serviceUrl, mediaPackageId, elementId, fileName);
    return new URI(destinationURI);
  }

  /**
   * Gets the directory containing the distributed files for this mediapackage.
   * 
   * @param mediaPackageId
   *          the mediapackage ID
   * @return the filesystem directory
   */
  protected File getMediaPackageDirectory(String mediaPackageId) {
    return new File(distributionDirectory, mediaPackageId);
  }

  protected boolean accepts(MediaPackageElement element){
    // HLS streaming servers only deal with tracks
    if (!MediaPackageElement.Type.Track.equals(element.getElementType())) {
      return false;
    }
    // The HLS ffmpeg profile only copies content so the input track must be h.264 or AAC
    TrackImpl track = (TrackImpl )element;
    if (track.getVideo().isEmpty() && track.getAudio().isEmpty()) {
       return false;
    } else if (!track.getVideo().isEmpty() && !Arrays.asList("H.264", "AVC").contains(track.getVideo().get(0).getFormat())) {
       return false;
    } else if (!track.getAudio().isEmpty() && !track.getAudio().get(0).getFormat().equals("AAC")) {
       return false;
    }
    return true;
  }

  /**
   * Callback for the OSGi environment to set the workspace reference.
   * 
   * @param workspace
   *          the workspace
   */
  protected void setWorkspace(Workspace workspace) {
    this.workspace = workspace;
  }

  /**
   * Callback for the OSGi environment to set the service registry reference.
   * 
   * @param serviceRegistry
   *          the service registry
   */
  protected void setServiceRegistry(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.opencastproject.job.api.AbstractJobProducer#getServiceRegistry()
   */
  @Override
  protected ServiceRegistry getServiceRegistry() {
    return serviceRegistry;
  }

  /**
   * Callback for setting the security service.
   * 
   * @param securityService
   *          the securityService to set
   */
  public void setSecurityService(SecurityService securityService) {
    this.securityService = securityService;
  }

  /**
   * Callback for setting the trusted HTTP client.
   * 
   * @param trustedHttpClient
   *          the trusted HTTP client to set
   */
  public void setTrustedHttpClient(TrustedHttpClient trustedHttpClient) {
    this.trustedHttpClient = trustedHttpClient;
  }

  /**
   * Callback for setting the user directory service.
   * 
   * @param userDirectoryService
   *          the userDirectoryService to set
   */
  public void setUserDirectoryService(UserDirectoryService userDirectoryService) {
    this.userDirectoryService = userDirectoryService;
  }

  /**
   * Sets a reference to the organization directory service.
   * 
   * @param organizationDirectory
   *          the organization directory
   */
  public void setOrganizationDirectoryService(OrganizationDirectoryService organizationDirectory) {
    this.organizationDirectoryService = organizationDirectory;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.opencastproject.job.api.AbstractJobProducer#getSecurityService()
   */
  @Override
  protected SecurityService getSecurityService() {
    return securityService;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.opencastproject.job.api.AbstractJobProducer#getUserDirectoryService()
   */
  @Override
  protected UserDirectoryService getUserDirectoryService() {
    return userDirectoryService;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.opencastproject.job.api.AbstractJobProducer#getOrganizationDirectoryService()
   */
  @Override
  protected OrganizationDirectoryService getOrganizationDirectoryService() {
    return organizationDirectoryService;
  }

}
