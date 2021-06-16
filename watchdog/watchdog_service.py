import os
from os import _exit as exit
from time import time, sleep
from redis_pal import RedisPal

SECOND = 1
MINUTE = 60 * SECOND
SLEEP_TIME = 30 * SECOND
SERVICES = {
    "docker": {
        "timeout": 5 * MINUTE,
        "keys": [
            "br_rj_riodejaneiro_brt_gps",
        ],
        "command": "systemctl restart docker"
    }
}


class RedisPalWdt (RedisPal):

    def service_timeout(self, service_name):
        timeout = SERVICES[service_name]["timeout"]
        keys = SERVICES[service_name]["keys"]
        max_delta = 0
        for key in keys:
            delta = time() - self.get_with_timestamp(key)["last_modified"]
            if (delta > max_delta):
                max_delta = delta
        return max_delta > timeout

    @classmethod
    def restart_service(cls, service_name):
        print("Need to restart service {}".format(service_name))
        os.system(SERVICES[service_name]["command"])

    def loop(self):
        try:
            for service in SERVICES.keys():
                print("Checking {}...".format(service), end="")
                if self.service_timeout(service):
                    print(" RESTARTING!")
                    self.restart_service(service)
                else:
                    print(" OK.")
        except Exception as e:
            print("ERROR: Exception has occured: {}".format(e))
            exit(1)
        sleep(SLEEP_TIME)


if __name__ == "__main__":
    try:
        sleep(5)
        wdt = RedisPalWdt()
        while True:
            wdt.loop()
    except Exception as e:
        print("Failed watchdog_service: {e}")
        print("Will wait for 5 seconds and retry...")
        sleep(5)
        exit(1)
