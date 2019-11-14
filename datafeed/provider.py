from abc import ABC, abstractmethod
from time import time, sleep
from threading import Lock

class Provider(ABC):

    # Get the number of requests permitted per minute
    @abstractmethod
    def get_limit(self):
        pass

    # Get the name of the data provider
    @abstractmethod
    def get_name(self):
        pass

    # Check whether we are able to make the request given our limitations
    @abstractmethod
    def make_request(self):
        pass

class AlphaVantageProvider(Provider):

    def __init__(self):
        self.count = 0
        self.lock = Lock()
        self.max_requests = 60
        self.name = "AlphaVantage"
        self.time = int(time()/60)

    def get_limit(self):
        return self.max_requests

    def get_name(self):
        return "AlphaVantage"

    def make_request(self):
        can_continue = False
        while not can_continue:
            self.lock.acquire()
            curr_time = int(time()/60)
            if curr_time == self.time:
                if self.count >= self.max_requests:
                    self.lock.release()
                else:
                    self.count += 1
                    self.lock.release()
                    can_continue = True
            else:
                self.time = curr_time
                self.count = 1
                self.lock.release()
                can_continue = True
            if not can_continue:
                print('[DataLink] Thread waiting as max requets have occurred...')
                sleep(10)
