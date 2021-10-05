# README #

### What is this repository for? ###

* This is the ProfileDataFormatter software development repo, containing the ProfileDataFormatter python application. The app is used to to format data from profiling mobile platforms (such as gliders and AUVs) into netCDF files for import into oceanographic instrument data repositories, such as the IOOS-DAC and OOI Data Explorer.

### Usage ###

* TBD

### Suppoprt ###

* For questions, please contact paul.whelan@whoi.edu

### Notes ###

 The soft link "configuration.py" in the root of this project
 points at ./legacy/gliderdac/configuration.py due to the 
 architecture of the legacy gliderdac application. It is referenced
 from nested packages within gliderdac, as though it were at the 
 root of the PYTHONPATH.