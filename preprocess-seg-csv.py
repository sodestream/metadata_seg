import numpy as np
import csv
import re
import json
import nltk

from ietfdata.datatracker import *
from ietfdata.mailarchive2 import *

import datetime
from datetime import datetime
from datetime import date
import pytz
#import seaborn as sns

import ietfdata.mailarchive2 as ma
import pickle
import pandas as pd

from joblib import Parallel, delayed

import shutil

from email_segmentation import iterate_over_thread, header_message_id

import traceback


jdata = json.load(open("config.json","r"))

archive = MailArchive(mongodb_username = jdata["mdb_uname"], mongodb_password = jdata["mdb_pass"])
MAILING_LISTS_TO_CONSIDER = list(archive.mailing_list_names())


# prefetch the raw texts of author emails
import email_segmentation

ext = email_segmentation.EnvelopeTextExtractor()

def generate_thread_segmentations(root_node):
    simpleseg = email_segmentation.SimpleEmailSegmenter()
    myseg = email_segmentation.EmailSegmenter()

    thread_nodes = iterate_over_thread(root_node)
    thread_nodes_text = []
    break_var = False
    for x, m in enumerate(thread_nodes):
        try:
            fh = m.header("from")[0] if len(m.header("from")) > 0 else "From field missing"
            thread_nodes_text += [(header_message_id(m), ext.extract(m), fh)]
        except:
            thread_nodes_text += [(header_message_id(m), "ERROR EXTRACTING TEXT", fh)]

    #thread_nodes_text = [(m.message_id, ext.extract(mid2rawtxt[m.message_id]), m.headers["From"]) for m in thread_nodes]
    #print([m.headers["From"] for m in thread_nodes])
    #print([m[1][:50] for m in thread_nodes_text])
    #print("------------------------")
    #print(len(thread_nodes_text))
    simple_thread_segmentations = simpleseg.segment_linear_thread(thread_nodes_text, verbose = False)
    #print(len(simple_thread_segmentations))
    thread_segmentations = myseg.segment_linear_thread(thread_nodes_text, verbose = False)
    #print(len(thread_segmentations))
       
    # add metadata
    thread_nodes_metadata = []
    for m in thread_nodes:
        fh = m.header("from")[0] if len(m.header("from")) > 0 else "From field missing"
        tf = m.header("to")[0] if len(m.header("to")) > 0 else "To field missing"
        subj = m.header("subject")[0] if len(m.header("subject")) > 0 else "Subject field missing"
        thread_nodes_metadata += [(header_message_id(m), fh, tf, m.date(), subj)]
        #print(m.header_message_id(), m.uid(), m.uidvalidity())

    return thread_segmentations, simple_thread_segmentations, thread_nodes_metadata







def copy_if_unchanged(mailing_list_name, old_seg_folder):
   
    copied = False
    total_msgs_count = -1
    
    
    ml = archive.mailing_list(mailing_list_name)
    try:
            if not ml:
                print("For %S there is no data in cache!" % (mailing_list_name))
            
            current_mid_set = set()
            if ml:        
                mlthreads = ml.threads(this_list_only = True)
                for thread_root_key in list(mlthreads.keys()):
                    thread_root = mlthreads[thread_root_key][0]
                    try:
                        thread_nodes = iterate_over_thread(thread_root)
                        for n in thread_nodes:
                            try:
                                current_mid_set.add(header_message_id(n))
                            except Exception as e:
                                print(e)
                    except Exception as e:
                        print(e)

            total_msgs_count = len(current_mid_set)

            meta_old = pd.read_csv(old_seg_folder + "/" + mailing_list_name + "-meta.csv")
            old_mid_set = set(meta_old["message_id"])

            num_diff = len(current_mid_set - old_mid_set) # these are the new messages that are present in current but not present in old

            total_msgs_count = len(current_mid_set)

            meta_old = pd.read_csv(old_seg_folder + "/" + mailing_list_name + "-meta.csv")
            old_mid_set = set(meta_old["message_id"])

            num_diff = len(current_mid_set - old_mid_set) # these are the new messages that are present in current but not present in old
            print("Number of differences in messages is %d" % (num_diff))
            
            if num_diff == 0:
                for sufix in ["-meta.csv", "-full.pickle", "-simple.pickle"]:
                        shutil.copyfile(old_seg_folder + "/" + mailing_list_name + sufix, "./segmented_texts/" + mailing_list_name + sufix)
                copied = True
                print("Copying succcessful!")
    except Exception as e:
            print(e)

    return copied, total_msgs_count
  



def process_list(mailing_list_name, current_seg_fol):
    output_results_meta = []
    output_dict_simple_seg = {}

    output_dict_full_seg = {}
    #mid2rawtxt = {}
    ml = archive.mailing_list(mailing_list_name)
    print("Working on " + ml.name())
    if not ml:
        print("For %S there is no data in cache!" % (mailing_list_name))
    
 
    if ml:
        ok2, notok2 = 0, 0
        mlthreads = ml.threads(this_list_only = True)

        for thread_root_key in list(mlthreads.keys()):
            thread_root = mlthreads[thread_root_key][0]
            try:
                seg_dict, simple_seg_dict, seg_metadata = generate_thread_segmentations(thread_root)    
                #print(len(simple_seg_dict))
                #print(len(seg_dict))
                #print(len(seg_metadata))
                
                assert len(simple_seg_dict) == len(seg_dict) == len(seg_metadata)
                    
                for i in range(len(simple_seg_dict)):                    
                        mid = seg_metadata[i][0]
                        output_results_meta.append(seg_metadata[i])
                        for k in simple_seg_dict:
                            output_dict_simple_seg[k] = simple_seg_dict[k]
                        for k in seg_dict:
                            output_dict_full_seg[k] = seg_dict[k]
                ok2 += 1
            except:
                print(" *** EXCEPTION FOR THREAD ***")
                traceback.print_exc()
                notok2 += 1
        out_df = pd.DataFrame(output_results_meta, columns = ["message_id", "from", "to", "date", "subject" ])
        out_df.to_csv(current_seg_fol + "/" + mailing_list_name + "-meta.csv")
        pickle.dump(output_dict_simple_seg, open(current_seg_fol + "/" + mailing_list_name + "-simple.pickle", "wb"))
        pickle.dump(output_dict_full_seg, open(current_seg_fol + "/" + mailing_list_name + "-full.pickle", "wb"))
 
    if ok2 + notok2 == 0:
        print("Finished for list " + mailing_list_name + " FAIL!")
    else:
        print("Finished for list %s --> %.1f%% threads were ok." % ( mailing_list_name, 100 * ok2 / (ok2 + notok2)))
 
    return output_results_meta, output_dict_simple_seg, output_dict_full_seg, ok2, notok2
         

PARALLEL = False # do not enable, seems buggy
TEST_MODE = False

start_letter = sys.argv[1]


old_seg_folder = "" #"/scratch-local/sodestream-text/email-segmented-text/segmented_texts"
current_seg_folder = "./segmented-texts"


if PARALLEL:
    start = time.time()
    Parallel(n_jobs=15, backend = "multiprocessing", prefer="processes")(delayed(process_list)(s) for s in MAILING_LISTS_TO_CONSIDER)
    end = time.time()
    print('{:.4f} s'.format(end-start))
else:
    for mailing_list_name in MAILING_LISTS_TO_CONSIDER:
        if mailing_list_name in ['ietf-announce','dns-privacy','detnet','hr-rt','pearg','smart','gen-art','gaia','i-d-announce','rtg-dir','ietf','gendispatch','hrpc']:
            continue
        if not mailing_list_name.startswith(start_letter):
            continue

        try:
            print("Trying to copy " + mailing_list_name)
            copied_old, total_msgs_present = copy_if_unchanged(mailing_list_name, old_seg_folder)
            if not copied_old:
                    meta, simple, full, ok2, notok2 = process_list(mailing_list_name, current_seg_folder)
                    print("***********************************************************************")
                    print("************** %d msgs in the list and %d msgs in the meta ************" % (total_msgs_present, len(meta)))
                    print("***********************************************************************")
            if TEST_MODE:
                for k in full.keys():
                    seg = full[k]
                    for s in seg:
                        print(s)
                    print("Serializing ...")
                    segs_json = SegmentationSerializer().serialize_to_json(full[k])
                    print(segs_json)
                    print()
                    print("Deserializing ...")
                    segs_back = SegmentationSerializer().deserialize_from_json(segs_json)
                    for s in segs_back:
                        print(s)
                    break

        except:
            print("*** UNHANDLED EXCEPTION AT THE LEVEL OF THE ENTIRE MAILING LIST -- " + mailing_list_name)
            traceback.print_exc() 
            exit()
            
        


            

