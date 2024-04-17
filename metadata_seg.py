from ietfdata.mailarchive2 import *
from email_segmentation import EmailSegment, SegmentationSerializer

def get_segmentation(m: Envelope, ma: MailArchive) -> list[EmailSegment]:
    # asdsad
    json_data = m.get_metadata("seg", "data")
    return SegmentationSerializer().deserialize_from_json(json_data)
    

def get_signature(m: Envelope, ma: MailArchive) -> str:
    segmentation = get_segmentation(m)
    return "\n ".join([seg.content for seg in segmentation if seg.type == "signature"])


