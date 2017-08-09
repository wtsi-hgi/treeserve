# treeserve

Sample lines from data file:

    L2x1c3RyZS9zY3JhdGNoMTE1	4096	13912	1313	1429959349	1469408402	1469408402	d	144115188193296385	8	907573372
    L2x1c3RyZS9zY3JhdGNoMTE1L3RlYW1zL21jZ2lubmlz	31	13912	1313	1435829520	1435829520	1435829520	l	1152921824397361177	1	907573372
    L2x1c3RyZS9zY3JhdGNoMTE1L3JlYWxkYXRhL21kdDAvcHJvamVjdHMvYXVzZ2VuL3ZycGlwZS1odW1hbl9ldm9sXzNiL291dHB1dC9lL2UvOS84NWFmZGZkN2U5YmQwMzYxYjA0NGQ5MTY3NDE0NS85MjkwNDUvNl9iYW1jaGVja19hdWdtZW50X3N1bW1hcnkvMTkzNzZfNyM3LmJhbS5iYW1jaGVjaw==	245068	13912	1568	1467117341	1467117210	1467117210	f	1152922193731071583	1	907573372


1.  base64 encoding of file/directory path  
    e.g. `L2x1c3RyZS9zY3JhdGNoMTE1L3JlYWxkYXRhL21kdDAvcHJvamVjdHMvYXVzZ2VuL3ZycGlwZS1odW1hbl9ldm9sXzNiL291dHB1dC9lL2UvOS84NWFmZGZkN2U5YmQwMzYxYjA0NGQ5MTY3NDE0NS85MjkwNDUvNl9iYW1jaGVja19hdWdtZW50X3N1bW1hcnkvMTkzNzZfNyM3LmJhbS5iYW1jaGVjaw==` (`/lustre/scratch115/realdata/mdt0/projects/ausgen/vrpipe-human_evol_3b/output/e/e/9/85afdfd7e9bd0361b044d91674145/929045/6_bamcheck_augment_summary/19376_7#7.bam.bamcheck`)
2.  object size in bytes  
    e.g. 245068 (~240KB)
3.  owner  
    e.g. 13912
4.  group  
    e.g. 1568
5.  atime  
    e.g. 1467117341
6.  mtime  
    e.g. 1467117210
7.  ctime  
    e.g. 1467117210
8.  object type - directory (d), symlink (l), normal file (f) etc.  
    e.g. f
9.  inode number  
    e.g. 1152922193731071583
10. number of hardlinks  
    e.g. 1
11. device ID  
    e.g. 907573372
