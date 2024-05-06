from ietfdata.mailarchive2 import *
from email_reply_parser import EmailReplyParser
from email.message import Message
import html2text
import copy
import time
import traceback
import json

def header_message_id(e):
    h = e.header("message-id")
    if len(h) > 0:
        return h[0]
    else:
        return None


class EmailSegment:
    def __init__(self):
        self.type = "unknown"
        self.content = ""
        self.antecedent = None
        self.id = None
        
    def from_params(i, text, ctype, a):
        c = EmailSegment()
        c.id = i
        c.content = text
        c.type = ctype
        c.antecedent = a
        return c

    def __str__(self):
        return "*** TYPE OF SEGMENT: %s **** \n\n %s \n ************************************** " % (self.type, self.content)
 
    def __repr__(self):
        return "*** TYPE OF SEGMENT: %s **** \n\n %s \n ************************************** " % (self.type, self.content)


class SegmentationSerializer:
    def __init__(self):
        pass
    
    def serialize_to_json(self, seg_list : list[EmailSegment]) -> str:

        l = []
        if seg_list is None:
            seg_list = []
        for s in seg_list:
            l.append({
                "id" : str(s.id),
                "aid" : str(s.antecedent),
                "stype" : s.type,
                "content" : s.content
            })
        return json.dumps(l)

    def deserialize_from_json(self, seg_list_json: str) -> list[EmailSegment]:
        seg_list = []
        parsed_segs = json.loads(seg_list_json)

        for parsed_seg in parsed_segs:
            cont = parsed_seg["content"]
            t = parsed_seg["stype"]
            i = int(parsed_seg["id"])
            ai = int(parsed_seg["id"]) if parsed_seg["id"].lower() != "none" else None
            seg = EmailSegment.from_params(i, cont, t, ai)
            seg_list.append(seg)

        return seg_list



class EmailTextExtractor:
    def __init__(self):
        h = html2text.HTML2Text()
        h.emphasis_mark = ""
        h.strong_mark = ""
        self.html_handler = h
    
    def email_to_text(self, email : Message) -> str:
        raw_body = email.get_body(preferencelist = ("plain","html"))
        clean_text_bytes = raw_body.get_payload(decode = True)        
        if raw_body.get_content_charset() is not None:
            clean_text = clean_text_bytes.decode(raw_body.get_content_charset())
        else:
            clean_text = clean_text_bytes.decode("utf-8")
        if raw_body.get_content_type() != "text/plain":
            if raw_body.get_content_type() == "text/html":
                clean_text = self.html_handler.handle(clean_text)
            else:
                raise Exception("Content type is something other than text or html.")
        return clean_text

class EnvelopeTextExtractor(EmailTextExtractor):
    def __init__(self):
        super().__init__()

    # will be used instead of segment_EmailMessage once we switch to new version of library
    def extract(self, email: Envelope) -> str:
        clean_text = self.email_to_text(email.contents())
        return clean_text

class VerbosePrinter:
    def __init__(self, v):
        self.verbose = v

    def print(self, s):
        if self.verbose:
            print(s)


class SimpleEmailSegmenter:
    def __init__(self):
        pass

    def segment_linear_thread(self, nodes, verbose = False):
        d = {}
        nextid = 0

        verbose = VerbosePrinter(verbose)
        
        verbose.print("SimpleSegmenter - doing newline based segmentation")
        for i in range(len(nodes)):

            verbose.print("SimpleSegmenter - starting for node "  + str(i))
            simpleseg = []            
            c = EmailSegment.from_params(nextid, nodes[i][1].strip(), "normal", None)
            simpleseg.append(c)
            d[nodes[i][0]] = simpleseg
            verbose.print("Finished for node " + str(i))
        return d


           
 
class EmailSegmenter:  

    def __init__(self):        
      pass

    def _combine_quotes(self, s : List[EmailSegment]) -> List[EmailSegment]:
        new_s, current_quote_list = [], []
        for segment in s:
            if segment.type != "quote":
                if len(current_quote_list) > 0:
                    new_seg = copy.deepcopy(current_quote_list[0])
                    new_seg.content = "\n".join([x.content for x in current_quote_list])
                    new_s.append(new_seg)
                    current_quote_list = []
                new_s.append(segment)
            else:
                current_quote_list.append(segment)
        if len(current_quote_list) > 0:
            new_seg = copy.deepcopy(current_quote_list[0])
            new_seg.content = "\n".join([x.content for x in current_quote_list])
            new_s.append(new_seg)
            current_quote_list = []
        return new_s
 
    def _handle_wrote(self, s : List[EmailSegment]) -> List[EmailSegment]:
        new_s = []
        for segment in s:
            new_seg = copy.deepcopy(segment)
            if segment.content.endswith(":") and ("wrote" in segment.content or "skrev" in segment.content or "écrit" in segment.content or "написал" in segment.content or "escribió" in segment.content or "寫了" in segment.content):
                new_seg.type = "quote"
            new_s += [new_seg]
        return new_s
    
    def _handle_quote_brackets(self, s : List[EmailSegment]) -> List[EmailSegment]:
        new_s = []
        for segment in s:
            new_seg = copy.deepcopy(segment)
            if segment.content.strip().startswith(">"):
                new_seg.type = "quote"
            new_s += [new_seg]
        return new_s


        
    def _handle_empty(self, s : List[EmailSegment]) -> List[EmailSegment]:
        return [x for x in s if len(x.content.strip()) > 0]

    def _handle_headers_and_dates(self,  s : List[EmailSegment]) -> List[EmailSegment]:        
        new_list = []
        for seg in s:
            c = seg.content
            if c.startswith("On Mon") or \
               c.startswith("On Tue") or \
               c.startswith("On Wed") or \
               c.startswith("On Thu") or \
               c.startswith("On Fri") or \
               c.startswith("On Sat") or \
               c.startswith("On Sun") or \
               c.startswith("From:") or \
               c.startswith("To:") or \
               c.startswith("Date:") or \
               c.startswith("Subject:") or \
               c.startswith("Sent:") or \
               "-Original Message-" in c or \
               c.lower().startswith("cc:") or \
               c.lower().startswith("bcc:"):
                seg.type = "quote"
            new_list.append(seg)
        return new_list

    def _hash_segment(self, s : str) -> str:
        return "".join(s.content.replace(">","").split()).lower().strip()

    def _join_segments(self, s : List[EmailSegment], idcounter: int) -> List[EmailSegment]:
        previous_segment = None
        current_segment_streak = []
        result_list = []

        tmp = s.copy()
        while True:
            if len(tmp) == 0:
                break
            if tmp[-1].type != "normal":
                tmp = tmp[0:-1]
            else:
                break
        no_quotes_do_aggressive_split = sum([x.type == "quote" for x in tmp ]) == 0

        current_seg_ind = 0
        for segment in s:
            current_seg_ind += 1

            if previous_segment == None:
                continue_streak = True
            else:
                if previous_segment.type == segment.type:
                    if segment.type == "normal" and segment.content.strip() == "" and previous_segment.content.strip() == "": 
                        continue_streak = False
                    else:
                        if segment.type == "normal" and segment.content.strip() == "":
                            if no_quotes_do_aggressive_split:
                                continue_streak = False
                            else:
                                continue_Streak = True
                        else:
                            continue_streak = True 
                else:
                    continue_streak = False

            if continue_streak:
                current_segment_streak.append(segment)
            else:
                if len(current_segment_streak) == 1:
                    new_seg = current_segment_streak[0]
                else:
                    new_seg = EmailSegment()               
                    new_seg.id = idcounter
                    idcounter +=1
                    new_seg.type = current_segment_streak[0].type # they all have the same type
                    new_seg.antecedent = self._pick_last_antecedent(current_segment_streak)
                    new_seg.content = "\n".join([s.content for s in current_segment_streak])
                    #if new_seg.id == 238:
                    #    print(",".join([str(s.id) for s in current_segment_streak]))
                result_list.append(new_seg)
                current_segment_streak = [segment]
            previous_segment = segment

        # TODO: refactor, the last streak doesn't get copied into the result, this fixes that
        if len(current_segment_streak) > 0:
                if len(current_segment_streak) == 1:
                    new_seg = current_segment_streak[0]
                else:
                    new_seg = EmailSegment()               
                    new_seg.id = idcounter
                    idcounter +=1
                    new_seg.type = current_segment_streak[0].type # they all have the same type
                    new_seg.antecedent = self._pick_last_antecedent(current_segment_streak)
                    new_seg.content = "\n".join([s.content for s in current_segment_streak])
                result_list.append(new_seg)
                current_segment_streak = []

        return result_list, idcounter

    def _pick_last_antecedent(self, s: List[EmailSegment]):
        return s[-1].antecedent

    def _pick_majority_antecedent(self, s:List[EmailSegment]):
        antecedent_counts = {}
        for seg in s:
            if seg.content.strip() == "":
                continue
            if seg.antecedent not in antecedent_counts:
                antecedent_counts[seg.antecedent] = 0
            antecedent_counts[seg.antecedent] += 1

        top, winner_id = 0, None
        for k in antecedent_counts:
            if antecedent_counts[k] >= top:
                top, winner_id = antecedent_counts[k], k

        return winner_id



    def _resolve_quotes(self,  s : List[EmailSegment], hist: List[EmailSegment]) -> List[EmailSegment]:        
        ret_list = copy.deepcopy(s)
        for cseg in ret_list:
            for hseg in hist:
              if self._hash_segment(cseg) in self._hash_segment(hseg) and cseg.content.strip().startswith(">"):
                   cseg.antecedent = hseg.id
                   cseg.type = "quote"
                   break

        return ret_list


    def _extract_name(self, s: str) -> str:
        ind = s.find("<")
        if ind > 0:
            s = s[:ind]
        s = s.replace("'","").replace('"', '')
        return s

    def _min(self, a,b):
        if a is None:
            return b
        if b is None:
            return a
        return min(a,b)


    def _handle_signatures(self, s : List[EmailSegment], auth_full: str) -> List[EmailSegment]:
        auth = self._extract_name(auth_full)
        #print(auth)
        ret_list = copy.deepcopy(s)
        sig_start_index = None
        sig_start_list = ["regards","sincerely","best wishes","cheers","best","as ever", "best regards", "kind regards"]
        # try to find line with Full name and not much else (up to 4 tokens total)
        for i in range(len(ret_list)):
            line_content = ret_list[i].content.strip().lower()
            if (auth.strip().lower() in line_content) and (len(line_content.split()) <= 4) and (not line_content.startswith(">")):
                sig_start_index = i
                #print("Found line with full name and nothing else.")
                break

        # try to find line with Best Cheers and only that (no comma)
        for i in range(len(ret_list)):
            for sig_start in sig_start_list:
                if ret_list[i].content.lower().startswith(sig_start):
                    sig_start_index = self._min(sig_start_index, i)
                    #print("Found line with sig. word and no comma.")
                    break

        # try to find line with "Best," "Cheers," (comma but may have something else after it)
        for i in range(len(ret_list)):
            for sig_start in sig_start_list + ["thanks"]:
                if ret_list[i].content.strip().lower().startswith(sig_start + ","):
                    sig_start_index = self._min(sig_start_index, i)
                    #print("Found line with sig. word and comma.")
                    break
        
        # try to find line containing only first name and nothing else
        firstname = auth.split()[0]
        for i in range(len(ret_list)):
            if ret_list[i].content.strip().lower() == firstname.lower().strip():
                sig_start_index = self._min(sig_start_index, i)
                #print("Found line with name and nothing else.")
                break
        
        # all normal segments till the next non normal segment are also signature (quote probably or possibly there are only normal segments all the way till the end)
        if sig_start_index is not None:
            for i in range(sig_start_index, len(ret_list)):
                ret_list[i].type = "signature"
        #print("-----")
        return ret_list

    def _handle_non_alnum(self, s : List[EmailSegment]) -> List[EmailSegment]:
        ret_list = []
        for seg in s:
            good = False
            for ch in seg.content:
                if ch.isalnum():
                    good = True
            if seg.content.strip() == "" or seg.content.strip() == "+":
                good = True
            if good:
                ret_list.append(seg)
        return ret_list

    def _count_newlines(self, s):
        return s.replace("\r\n", "\n").count("\n")
 
    def segment_linear_thread(self, nodes, verbose = False):
        d = {}
        nextid = 0

        verbose = VerbosePrinter(verbose)

        for i in range(len(nodes)):
            try:
           
                verbose.print("*" * 80)
                verbose.print("Starting newline segmentation procedure for node " + str(i))

                # segment by new lines, each line is its own segment
                newlineseg = []
                for s in nodes[i][1].split("\n"):
                    c = EmailSegment.from_params(nextid, s.strip(), "normal", None)
                    nextid += 1
                    newlineseg.append(c)

                verbose.print("Trying to get author name")
                author_name = nodes[i][2]

                verbose.print("Trying to get author name")
                author_name = nodes[i][2]

                # remove headers and dates
                verbose.print("Handling headers and dates")
                segments2 = self._handle_headers_and_dates(newlineseg)

                #remove wrote
                verbose.print("Handling 'Wrote:' parts")
                segments3 = self._handle_wrote(segments2)

                #remove wrote
                verbose.print("Handling quote brackets")
                segments4 = self._handle_quote_brackets(segments3)
                hist_list = []
                for k in d:
                    hist_list += d[k]

                verbose.print("Resolving quotes")
                segments5 =  self._resolve_quotes(segments4, hist_list)

                verbose.print("Handling non alphanumeric parts")
                segments6 = self._handle_non_alnum(segments5)

                verbose.print("Handling signatures")
                segments7 = self._handle_signatures(segments6, author_name)
                # handle -- original message --
                for j in range(len(segments7)):
                    if "-original message-" in segments7[j].content.lower():
                        for k in range(i, len(segments7)):
                            segments7[k].type = "quote"
                    break

                # join up segments
                verbose.print("Joining result segments")
                segments8, nextid = self._join_segments(segments7, nextid)
                #segments8 = segments7
                
                verbose.print("Shortening signatures")
                last_normal = None
                for j in range(len(segments8)):
                    if segments8[j].type == "normal" and segments8[j].content.strip() != "":
                        last_normal = j
                if last_normal is not None:
                    # create new segment which will hold the signature (but ideally not all the trailing qutoes)
                    sig_segment = EmailSegment()
                    sig_segment.id = nextid
                    nextid += 1
                    sig_segment.content = self._extract_name(author_name) + " <SIGNATURES AND QUOTES>"
                    sig_segment.content += "\n\n"
                    sig_segment.type = "signature"

                    # fill it with up to 15 lines from below the beginning of the detected signature
                    lines_added = 0
                    finished = False
                    if len(segments8) > last_normal + 1: 
                        for j in range(last_normal + 1,len(segments8)):
                            if finished:
                                break
                            for l in segments8[j].content.replace("\r\n","\n").split("\n"):
                                sig_segment.content += l + "\n"
                                lines_added += 1
                                if lines_added >= 15:
                                    finished = True
                                    break

                    # substitute the signature and all trailing quote segments with the new compact signature segment we just made
                    segments8 = segments8[:last_normal + 1]
                    segments8 += [sig_segment]
                verbose.print("Adding result into result list")   
                d[nodes[i][0]] = segments8
                #print("Successfully added segmentation for id = " + nodes[i][0])
            except:
                d[nodes[i][0]] = None
                print("Problem for single tree node")
                traceback.print_exc()
        return d


def iterate_over_thread(node: Envelope) -> list[Envelope]:
    visited_ids, results, queue = set(), [], [node]

    starting_ml_name = node.mailing_list().name()
    
    while len(queue) > 0:       
        current_node = queue.pop(0)
        curr_id = header_message_id(current_node)
        if curr_id in visited_ids or curr_id is None: # curr_id == None seems to make the db replies() request below hang
            continue

        results.append(current_node)
        visited_ids.add(curr_id)
        
        direct_children = current_node.replies()
        for child in direct_children:
            if child.mailing_list().name() == starting_ml_name:
                queue.append(child)

    return results
 

#def iterate_over_thread(node: Envelope) -> list[Envelope]:
#    print("Calling get replies")
#    direct_children = node.replies()
#    all_children_list = []
#    visited_children_mids = set()
#    print("ROOT NODE " + str(node.header_message_id()))
#    for child in direct_children:               
#        print("CHILD OF  " + str(node.header_message_id()) + " is " + str(child.header_message_id()))
#        cmid = child.header_message_id()
#        if cmid not in visited_children_mids: 
#            visited_children_mids.add(cmid)
#            all_children_list += iterate_over_thread(child)
#    return [node] + all_children_list



   
#def iterate_over_thread_rec(node: Envelope) -> list[Envelope]:
#    print("Calling get replies")
#    direct_children = node.replies()
#    all_children_list = []
#    print("ROOT NODE " + str(node.header_message_id()))
#    for child in direct_children:               
#        print("CHILD OF  " + str(node.header_message_id()) + " is " + str(child.header_message_id()))
#        all_children_list += iterate_over_thread(child)
#    return [node] + all_children_list

#def iterate_over_thread(node: Envelope) -> list[Envelope]:
#    res = iterate_over_thread_rec(node)
#    seen_ids, final_res = set(), []
#    for n in res:
#        nid = n.header_message_id()
#        if nid not in seen_ids:
#            seen_ids.add(nid)
#            final_res.append(n)
#    return final_res

def get_unquoted_texts(arch : MailArchive, list_name : str, allowed_ids = None) -> Dict[str, str]:
  text_ext =  MailingListMessageTextExtractor()
  segmenter = EmailSegmenter()

  ml = arch.mailing_list(list_name)
  
  hist_segmentations = {}
  n_threads = len(ml.threads())
  print()
  for tn, thread_root in enumerate(ml.threads()):
      print("\rProcessing thread %d / %d" % (tn, n_threads), end = '')
      thread_history = []
      thread_messages = iterate_over_thread(thread_root)
      for msg in thread_messages:
          try:
            mid = header_message_id(msg)        
            if allowed_ids is not None:
                if mid not in allowed_ids:
                    continue
            current_text = text_ext.extract(msg)
            current_segmentation = segmenter.segment_text(current_text)
            hist_segmentations[mid] = segmenter.historical_resegmentation(current_segmentation, thread_history)
            thread_history.append(current_segmentation)
          except:
            print("WARNING: message with id %s in list %s caused an error when doing history based segmentation" % (mid, list_name))
  
  final_result = {}
  for mid in hist_segmentations:
      final_result[mid] = segmenter.segmentaton_to_unquoted_text(hist_segmentations[mid])
  return final_result
          





 
    

if __name__ == "__main__":

    seg = EmailSegmenter()
    archive = MailArchive(mongodb_username = "admin", mongodb_password = "DzKvurBMsKtAEOQ9s9r")

    # *** Example on text ***
    print("*********************** Example 1 extracting unquoted parts from raw text  *********************************************************")
    test_email_text = "> This is\n> A quote \nAnd this is the reply.\n\n---\nHenry Jones Junior, PhD"
    segments = seg.segment_text(test_email_text)
    print("Segments:")
    for c in segments:
        print("*** TYPE: " + c.type + "***")
        print(c.content)

    print("\n\n\n AFTER REMOVING QUOTES:")
    # to get only the original (non-quoted) bits you can do 
    new_text = seg.segmentation_to_unquoted_text(segments)
    print(new_text)




    ml = archive.mailing_list("avt")
    msgid_of_interest = "<3B38A38F.85BAA3CD@cisco.com>"

    # *** Example on Message - old way, will work without updating mailarchive.py ***
    print("\n\n\n\n*********************** Example 2 extracting unquoted parts from  EmailMessage class (old lib version)*********************************************************")
    for i in ml.message_indices():    
        raw_msg = ml.raw_message(i)
        if 'message-id' in raw_msg:        
            if raw_msg["message-id"] == msgid_of_interest:
                ext = EmailMessageTextExtractor()
                text = ext.extract(raw_msg)
                segments = seg.segment_text(text)
                print("Segments:")
                for c in segments:
                    print("*** TYPE: " + c.type + "***")
                    print(c.content)
                print("\n\n\n AFTER REMOVING QUOTES:")
                print(seg.segmentation_to_unquoted_text(segments)) 
                break
        

    # *** Example on Message - old way, will work without updating mailarchive.py ***
    print("\n\n\n\n*********************** Example 3 extracting unquoted parts from entire mailing list accounting for history (old lib version) *********************************************************")
    print()
    print(list(result.values())[3])
    print(len(result))
    print("Time: " + str(time.time() - t_start))



    # *** Example on Message - new (preferred) way, you might need to update your mailarchive.py if you have an old version ***
    # updating mailarchive.py might cause your cache to rebuild which might take quite a while (hours), but will only run once - just let it finish
    print("\n\n\n\n*********************** Example 4 extracting unquoted parts from  MailingListMessage class (new lib version) *********************************************************")
    #for msg in ml.messages():
    #    print(msg.message_id)
    #    if msg.message_id == msgid_of_interest:
    #        ext = MailingListMessageTextExtractor()
    #        text = ext.extract(msg)
    #        segments = seg.segment_text(text)
    #        for c in segments:
    #             print("*** TYPE: " + c.type + "***")
    #             print(c.content)
    #        print("\n\n\n AFTER REMOVING QUOTES:")
    #        print(seg.segmentation_to_unquoted_text(segments))  
    #        break

    # *** Example on Message - old way, will work without updating mailarchive.py ***
    print("\n\n\n\n*********************** Example 5 extracting unquoted parts from entire mailing list accounting for history (new lib version) *********************************************************")

    #result = get_unquoted_texts_v2(archive, "avt")
    #print()
    #print(list(result.values())[5])


