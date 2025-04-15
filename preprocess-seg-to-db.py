from ietfdata.mailarchive2 import *
import traceback
import pickle
from email_segmentation import SegmentationSerializer, iterate_over_thread, header_message_id
import json

total_start_time = time.time()

jdata = json.load(open("config.json","r"))
ma = MailArchive(mongodb_username = jdata["mdb_uname"], mongodb_password = jdata["mdb_pass"])

TEST_MODE = False

if __name__ == "__main__":
    for ml_name in ma.mailing_list_names():
        if TEST_MODE:
            if ml_name != "100attendees":
                continue 

        #print("Working on list:" + ml_name)
        ml = ma.mailing_list(ml_name)
        try:
            mid2seg = pickle.load(open("./segmented-texts/" + ml_name + "-full.pickle", "rb"))            
        except:
            print("Error loading pickle for " +  ml_name)
            traceback.print_exc()
            continue

        total, no = 0, 0
        mlthreads = ml.threads(this_list_only = True)
        for thread_root_key in list(mlthreads.keys()):
            thr_root = mlthreads[thread_root_key][0]

            for msg in iterate_over_thread(thr_root):
                total+=1 
                mid = header_message_id(msg)
                if mid not in mid2seg:
                    no += 1
                    continue
                
                seg_json = SegmentationSerializer().serialize_to_json(mid2seg[mid])

                msg.clear_metadata("seg")
                msg.add_metadata("seg", "data", seg_json)
        
        if no + total != 0:
            print("Finished for mailing list %s --> Number of missing segmentations %d / %d (%.3f)" % (ml_name, no, total, no / total))
        #print()
      
    


