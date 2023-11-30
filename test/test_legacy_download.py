#!/usr/bin/env python

"""
Python script to download selected data files from rda.ucar.edu.
Number of files selected: 2
Data volume: 1.82G
RDA dataset: ds084.1
Request index: 713459

After you save the file, don't forget to make it executable
  i.e. - "chmod 755 <name_of_script>"

Contact rdahelp@ucar.edu (RDA help desk) for further assistance.
"""

import sys, os
from urllib.request import build_opener

opener = build_opener()
dspath = 'https://request.rda.ucar.edu/dsrqst/STENDE713459/'
filelist = [
  #'TarFiles/gfs.0p25.2023110100.f003-25.2023110512.f207.grib2.tar',
  'TarFiles/gfs.0p25.2023110512.f210-25.2023110600.f384.grib2.tar'
]

for file in filelist:
   filename = dspath + file
   ofile = os.path.basename(filename)
   sys.stdout.write("downloading " + ofile + " ... ")
   sys.stdout.flush()
   infile = opener.open(filename)
   outfile = open(ofile, "wb")
   outfile.write(infile.read())
   outfile.close()
   sys.stdout.write("done\n")