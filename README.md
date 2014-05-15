warcutils
=========

Library with utility classes for working with the March 2014 Common Crawl warc, wet and wat files on the SURFsara hadoop cluster environment.

Introduction
------------
The March 2014 Common Crawl data is provided in the form of warc, wet and wat files. For the file format specification see: [http://bibnum.bnf.fr/WARC/](http://bibnum.bnf.fr/WARC/).

Multiple parser exist for this format, we have chosen to use and provide utilities which use the Java Web Archive Toolkit libraries: [https://sbforge.org/display/JWAT/JWAT](https://sbforge.org/display/JWAT/JWAT). Both readers and inputformats for regular warc.gz, warc.wat.gz and warc.wet.gz files as well as readers and inputformats for warcfiles which have been converted to sequencefiles (for more information on this see: [this description of the data](http://norvigaward.github.io/examples.html#data)) are provided. In addition two implementations of Pig load functions have been provided (one for regular files and one for sequence files).

Building
--------
Both Ant&Ivy as wel as Maven build files are provided with the project. The latest builds can also be obtained from our maven repository: [http://beehub.nl/surfsara-repo/releases/SURFsara/warcutils](http://beehub.nl/surfsara-repo/releases/SURFsara/warcutils)

Usage
-----
Please see the warcexamples project: [https://github.com/norvigaward/warcexamples](https://github.com/norvigaward/warcexamples). For some examples that use this library.
