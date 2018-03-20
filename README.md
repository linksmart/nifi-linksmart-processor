# Linksmart Nifi Processor

This is a Nifi processor which functions similar to the **Linksmart Gateway**. A user can specify a sub-process to run under this processor. This processor will catch the `stdout` of that sub-process and propagate the information into the Nifi system. This processor has the following properties:

|Property Name | Description |
|--|--|
|Command Line | The command line to be executed in a subprocess. |
|Maximum Message Queue Size | The maximum number of messages to add to a single FlowFile. If multiple messages are available, they will be concatenated along with the <Message Delimiter> up to this configured maximum number of messages|
|Maximum Batch Size |The maximum number of messages to add to a single FlowFile. If multiple messages are available, they will be concatenated along with the <Message Delimiter> up to this configured maximum number of messages |
|Batching Message Delimiter |Specifies the delimiter to place between messages when multiple messages are bundled together (see <Max Batch Size> property). |


## Build Instructions
Build it with Maven:
```
mvn clean install
```
Find the built nar file here:
```
<repo directory>/nifi-custom-listen-tcp-nar/target/nifi-linksmart-nar-<version number>.nar
```
and copy it to the following directory of the running Nifi instance:
```
/opt/nifi/nifi-1.4.0/lib
```
Restart Nifi, then you can find the new ``LinksmartTCP`` processor available. You can specify the delimiter for separating incoming messages in the property field `Incoming Message Delimiter`.

## Example for Sub-process
In principle, as long as a sub-process writes data to its `stdout` channel, it could be used with the `LinksmartProcessor`. Typically, a user can specify a script, e.g. a Python script, to be run as a sub-process. In that script, some sensor data will be read and published to the `stdout` channel. In practice, the sub-process should follow the following principles:  
  

 1. The sub-process should be a long running program.
 2. The sub-process should publish useful information to `stdout` and error messages to `stderr`, so that the error messages could also be propagated to Nifi interface.
 3. The sub-process should catch the `SIGTERM` signal, because this is the signal the `LinksmartProcessor` send to the sub-process, when it tries to terminate it. The sub-process should have a callback function which shut itself down gracefully (closing opened resources/connections, etc.).

As an example, this is a Python script which fits the above principles:
```python
import signal
import sys
import time

# Callback for SIGTERM signal
def sigterm_handler(signal, frame):
    # Release resources and shutdown program
    # ...
    sys.exit(0)
    
# Catch SIGTERM signal
signal.signal(signal.SIGTERM, sigterm_handler)

# A long running loop which keeps publishing data to stdout
while True:
	# Do your stuff
    print('new data')
    sys.stdout.flush() # Since Python caches its stdout, you need to flush it explicitly 
    time.sleep(1)
```

Let's assume the above script is saved as`/scripts/dummy.py` in the same host as the running Nifi instance. Then in the `Command Line` property of the `LinksmartProcessor`, you can fill in `python /scripts/dummy.py`. Run the processor, and the message will be published by the `LinksmartProcessor`.