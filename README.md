warcutils
=========

Library with utility classes for working with the March 2014 Common Crawl warc, wet and wat files on the SURFsara hadoop cluster environment.

Introduction
------------
The March 2014 Common Crawl data is provided in the form of warc, wet and wat files. For the file format specification see: [http://bibnum.bnf.fr/WARC/](http://bibnum.bnf.fr/WARC/). 

Multiple parser exist for this format, we have chosen to use and provide utilities which use the Java Web Archive Toolkit libraries: [https://sbforge.org/display/JWAT/JWAT](https://sbforge.org/display/JWAT/JWAT). Both readers and inputformats for regular warc.gz, warc.wat.gz and warc.wet.gz files as readers and inputformats for warcfiles which have been converted to sequencefiles (for more information on this see: []()) are provides. In addition two implementations of Pig loader functions have been provided (one for regular files and one for sequence files).   

Building
--------
A basic ant buildfile is included and dependency resolution is provided by ivy. A precompiled bundle of this project and necessary dependencies can be found here: []()  

Usage
-----
Please see the warcexamples project: [](). For details on using this library.


