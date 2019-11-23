from abc import ABC, abstractmethod
from time import time, sleep
from threading import Lock

class Provider(ABC):

    def __init__(self):
        self.count = 0
        self.lock = Lock()
        self.max_requests = 0
        self.time = int(time()/60)

    def get_count(self) -> int:
        """Returns the number of requests that have been made in the last minute
        
        Returns:
            int -- The number of requests made
        """
        return self.count

    def get_limit(self) -> int:
        """Returns the number of requests per minute permitted by the provider.
        
        Returns:
            int -- The number of requests permitted.
        """
        return self.max_requests

    @abstractmethod
    def get_name(self) -> str:
        """Returns the name of the data provider.
        
        Returns:
            str -- The name of the data provider.
        """

    def get_unused_quota(self) -> int:
        """Returns the number of requests that can still be made in the current minute.
        
        Returns:
            int -- The number of requests that can be made in the next minute.
        """
        return self.max_requests-self.count

    def make_request(self) -> None:
        """Determines whether a request can be made at this point in time; if it cannot
        the thread will sleep until it can make the request.
        """
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

class AlphaVantageProvider(Provider):

    def __init__(self):
        self.count = 0
        self.lock = Lock()
        self.max_requests = 60
        self.name = "AlphaVantage"
        self.time = int(time()/60)

    def get_name(self):
        return self.name

class GoogleTrendsProvider(Provider):

    def __init__(self):
        self.count = 0
        self.lock = Lock()
        self.max_requests = 5
        self.name = "GoogleTrends"
        self.time = int(time()/60)
    
    def get_name(self):
        return self.name
