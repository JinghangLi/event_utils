import glob
import argparse
import rosbag
import rospy
import pandas as pd
import math
import os

from std_msgs.msg import Time
from strttc_msgs.msg import boundingbox_message
from strttc_msgs.msg import ttc_message
from dvs_msgs.msg import EventArray, Event


def get_frame_from_fullpath(path: str) -> int:
    filename = os.path.splitext(os.path.split(path)[-1])[0]
    part = filename.split("_")[-1]
    return int(part)


def main(vehicle_name):
    base_dir = "/mnt/disk1/dataset/Carla/e2depth/20230601_125609_ConstVel_10_type1"
    bbox_file = f"{base_dir}/{vehicle_name}/bbox/bbox.csv"
    ttc_file = f"{base_dir}/{vehicle_name}/ttc/gt_ttc.csv"

    bbox_df = pd.read_csv(bbox_file, names=["frame", "ts_secs", "xmin", "ymin", "xmax", "ymax"])
    ttc_df = pd.read_csv(ttc_file, names=["frame", "ts_secs", "distance", "velocity", "ttc"])
    timestamp_df = ttc_df[["frame", "ts_secs"]]

    events_file_list = sorted(glob.glob(f"{base_dir}/{vehicle_name}/txt/*.txt"))
    save_list = []
    for events_rawdata_path in events_file_list:
        frame = get_frame_from_fullpath(events_rawdata_path)
        save_list.append({'frame': frame,
                          'events_rawdata_path': events_rawdata_path})
    events_rawdata_df = pd.DataFrame(save_list)
    events_rawdata_df = pd.merge(events_rawdata_df, timestamp_df, on="frame")

    rosbag_path = f"{base_dir}/{vehicle_name}.bag"

    with rosbag.Bag(rosbag_path, 'w') as bag:
        # Write bbox
        for index, row in bbox_df.iterrows():
            bbox = boundingbox_message()

            ts = row['ts_secs']
            secs = math.floor(ts)  # 向下取整得到秒数
            nsecs = math.floor((ts - secs) * 1e9)

            ros_ts = rospy.Time(secs, nsecs)

            stamp = Time()
            stamp.data.secs = int(secs)
            stamp.data.nsecs = int(nsecs)

            bbox.stamp = stamp
            bbox.xmin = int(row['xmin'])
            bbox.ymin = int(row['ymin'])
            bbox.xmax = int(row['xmax'])
            bbox.ymax = int(row['ymax'])

            print(f"Saved: ts: {ros_ts.secs}_{ros_ts.nsecs} bbox: {bbox.xmin}_{bbox.ymin}_{bbox.xmax}_{bbox.ymax}")
            bag.write('/bbox', bbox, t=ros_ts)
            print(index)

        # Write TTC
        for index, row in ttc_df.iterrows():
            ttc = ttc_message()

            ts = row['ts_secs']
            secs = math.floor(ts)  # 向下取整得到秒数
            nsecs = math.floor((ts - secs) * 1e9)

            ttc.stamp.data.secs = int(secs)
            ttc.stamp.data.nsecs = int(nsecs)
            ttc.ttc.data = float(row['ttc'])

            ros_ts = rospy.Time(secs, nsecs)
            print(f"Saved: ts: {ros_ts.secs}_{ros_ts.nsecs} ttc: {ttc.ttc.data}")
            bag.write('/ttc', ttc, t=ros_ts)

        # Write events
        for index, row in events_rawdata_df.iterrows():
            event_array_file = row['events_rawdata_path']
            event_array_ts = float(row['ts_secs'])
            event_array_ros_ts = rospy.Time.from_seconds(event_array_ts)

            event_array = EventArray()

            secs = math.floor(event_array_ts)  # 向下取整得到秒数
            nsecs = math.floor((event_array_ts - secs) * 1e9)
            event_array.header.stamp.secs = int(secs)
            event_array.header.stamp.nsecs = int(nsecs)
            event_array.height = int(480)
            event_array.width = int(640)

            with open(event_array_file, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    line = line.strip('\n')
                    event = Event()
                    t_secs = float(line.split(' ')[0])
                    e_ts = rospy.Time.from_seconds(t_secs)
                    event.ts = e_ts
                    event.x = int(float(line.split(' ')[1]))
                    event.y = int(float(line.split(' ')[2]))
                    pol = int(line.split(' ')[3])
                    if pol == -1:
                        pol = 0
                    event.polarity = bool(pol)
                    event_array.events.append(event)

            print(f"Saved: ts: {event_array_ros_ts.to_nsec()} events_length: {len(event_array.events)}")
            bag.write('/events', event_array, t=event_array_ros_ts)
    bag.close()


if __name__ == "__main__":
    name_list = ['Ambulance', 'CarlaCola', 'Patrol', 'Sprinter', 'Firetruck', 'Rubicon', 'Volkswagen']
    for name in name_list:
        main(name)
