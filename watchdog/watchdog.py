import os
import yaml
import requests
from os import _exit as exit
from time import time, sleep
from redis_pal import RedisPal


class RedisPalWdt (RedisPal):

    def print_and_send_to_discord(self, msg):
        print(msg)
        requests.post(self.discord_webhook, data={'content': msg})

    def setup(self):
        self.config: dict = None
        with open("/app/watchdog_config.yaml", "r") as f:
            self.config = yaml.load(f, Loader=yaml.Loader)
        self.services: dict = self.config["services"]
        self.config: dict = self.config["config"]
        self.discord_webhook: str = os.environ["WDT_DISCORD_WEBHOOK"]

    def cooldown(self):
        print(f'Cooldown: sleep for {self.config["cooldown"]} seconds')
        sleep(self.config["cooldown"])

    def service_timeout(self, service_name):
        timeout = self.services[service_name]["timeout"]
        keys = self.services[service_name]["keys"]
        max_delta = 0
        for key in keys:
            delta = time() - self.get_with_timestamp(key)["last_modified"]
            if (delta > max_delta):
                max_delta = delta
        return max_delta > timeout

    def execute_timeout_command(self, service_name):
        self.print_and_send_to_discord("Service {} triggered the watchdog, will execute \"{}\"".format(
            service_name, self.services[service_name]["command"]))
        try:
            os.system(self.services[service_name]["command"])
            self.print_and_send_to_discord("Successfully executed command")
        except Exception as e:
            self.print_and_send_to_discord(
                f"Failed to execute command! Error message: {e}.")
        self.cooldown()

    def loop(self):
        try:
            for service in self.services.keys():
                print("Checking {}...".format(service), end="")
                if self.service_timeout(service):
                    print(" TIMEOUT EXCEEDED!")
                    self.execute_timeout_command(service)
                else:
                    print(" OK.")
        except Exception as e:
            print("ERROR: Exception has occured: {}".format(e))
            exit(1)
        sleep(self.config["loop_time_delta"])


if __name__ == "__main__":
    try:
        wdt = RedisPalWdt(host="redis")
        wdt.setup()
        wdt.cooldown()
        while True:
            wdt.loop()
    except Exception as e:
        print(f"Failed watchdog_service: {e}")
        exit(1)
