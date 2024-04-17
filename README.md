# metadata-seg
Code to add segmentation of message text for messages from the IETF mailarchive. 

To generate segmentations install the ietfdata library available at [this link](https://github.com/glasgow-ipl/ietfdata/).

Then checkout this repository and run:

`./preprocess-seg-to-csv.sh`

This may take some time (possibly a few hours). Then run:

`python preprocess-seg-to-db.py

This will write the results to the your mongodb mailarchive2 instance.
You can now access the segmentations using this code:

```
ml_name = "100all"
ml = archive.mailing_list(ml_name)
if ml:
   threads_dict = ml.threads(this_list_only = True)
   for thread_root_msg in threads_dict.values():
       for msg in iterate_over_thread(thread_root_msg):                            
           # segmentations 
           for segment in get_segmentation(msg):
               print("Segment %d is quoting segment %d" % (segment.id, segment.antecedent))
               print()
               print("Content of segment:")
               print(segment.content)
               print()
               print("Type of segment:")
               print(segment.type)
```
