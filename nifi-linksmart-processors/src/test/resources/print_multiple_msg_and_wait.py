import sys
import time

for x in range(0, 3):
    print("Msg " + str(x) + " printed by Python");
    sys.stdout.flush()

time.sleep(10)