# README #

### What is this repository for? ###

* This is the ProfileDataFormatter software development repo, containing the ProfileDataFormatter python application. The app is used to to format data from profiling mobile platforms (such as gliders and AUVs) into netCDF files for import into oceanographic instrument data repositories, such as the IOOS-DAC and OOI Data Explorer. The application is derived from work done by Stuart Pearce at OSU <contact info> processing Slocum Glider data for import into IOOS-DAC. That gliderdac application is imported as a git subtree under legacy in this repository. The gliderdac code is used "as is" to implement formatting for the Slocum glider.

### Usage ###

* TBD

### Support ###

* For questions, please contact paul.whelan@whoi.edu

### Notes ###

 The soft links "configuration.py", "profile_filters.py" and "ooidac" in the root of this project
 point at ./legacy/gliderdac code and directories due to the architecture of the legacy gliderdac application. 
 They are referenced from nested packages within gliderdac, as though it were at the 
 root of the PYTHONPATH. These were required to utilize the gliderdac code without modification.
