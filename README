Generate tree structure from the mpistat output and allow querying of it via a rest api.

Original C++ version
====================

Dependencies (not complete):
* Needs facebooks proxygen http server library
* Boost

Using proxygen brings in the google logging and command-line options libraries.
These give a lot of extra command-line options. Use --help to list them.

A good commandline to use would be something like...

bin/treeserve -lstat bin/114_1.dat.gz -dump=bin/tree.bin -logtostderr -gzip_buf 64 -port 8000

Format of fields in the data file are :

* a prefix (the lustre volume number)
* base64 encoding of the path (to handle unprintable characters in paths)
* size of the object
* owner
* group
* atime
* mtime
* ctime
* object type (dir, normal file, symlink etc.)
* inode #
* number of hardlinks
* device id

Python version (using LMDB)
===========================

###Requirements

    python 3.4+
    pip install -r requirements.txt

Format of fields in the data file are :

* base64 encoding of the path (to handle unprintable characters in paths)
* size of the object
* owner
* group
* atime
* mtime
* ctime
* object type (dir, normal file, symlink etc.)
* inode #
* number of hardlinks
* device id

LMDB
----

LMDB data persists between runs, to recalculate data remove the cache directory.


Go version (also using LMDB)
============================

There is also a go version. 

