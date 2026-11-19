
import time

from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows


# Extract timestamp from each event
class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return event[1]


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Delivery events: (event name, timestamp)
events = [
    ("Picked up", 1000),
    ("At restaurant", 8000),
    ("Delivered", 22000),
    ("Order confirmed", 3000)
]

data = env.from_collection(
    events,
    type_info=Types.TUPLE([Types.STRING(), Types.LONG()])
)

# Watermark: allow 2 seconds of out-of-order events
watermarks = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(2)
).with_timestamp_assigner(MyTimestampAssigner())


# Simulate delayed arrival
def delay(event):
    time.sleep(5 if event[0] == "Order confirmed" else 1)
    return event


data = data.map(
    delay,
    output_type=Types.TUPLE([Types.STRING(), Types.LONG()])
)

stream = data.assign_timestamps_and_watermarks(watermarks)

# Side output for late events
late_tag = OutputTag(
    "late",
    Types.TUPLE([Types.STRING(), Types.LONG()])
)

# Create 10-second tumbling windows
windowed = (
    stream
    .key_by(lambda x: "delivery", key_type=Types.STRING())
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .side_output_late_data(late_tag)
)

# Combine events in each window
result = windowed.reduce(
    lambda a, b: (a[0] + " + " + b[0], max(a[1], b[1])),
    output_type=Types.TUPLE([Types.STRING(), Types.LONG()])
)

# Display normal and late events
result.print("MAIN OUTPUT")
result.get_side_output(late_tag).print("LATE EVENTS")

env.execute("Food Delivery Late Data")
