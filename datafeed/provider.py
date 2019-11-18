from abc import ABC, abstractmethod
from time import time, sleep
from threading import Lock

class Provider(ABC):

    @abstractmethod
    def get_limit(self) -> int:
        """Returns the number of requests per minute permitted by the provider.
        
        Returns:
            int -- The number of requests permitted.
        """

    @abstractmethod
    def get_name(self) -> str:
        """Returns the name of the data provider.
        
        Returns:
            str -- The name of the data provider.
        """

    @abstractmethod
    def make_request(self) -> None:
        """Determines whether a request can be made at this point in time; if it cannot
        the thread will sleep until it can make the request.
        """

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
                print('[DataLink] Thread waiting as max requests have occurred...')
                sleep(10)
