import os
path_video = "datasets/aic_2022/videos/S02/c006.avi"
if os.path.exists(path_video):
    print("video file exists")
else:
    print("video file does not exist")

# Frame --> topic raw_frames --> detetctions (YOLO) --> topic detections -->  mot (tracking) --> topic mot --> mtmc --> topic final