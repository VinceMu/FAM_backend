from datetime import datetime
from typing import Dict

from mongoengine import BooleanField, DateTimeField, Document, IntField, StringField

class Trend(Document):

    search_term = StringField(required=True)
    timestamp = DateTimeField()
    is_partial = BooleanField()
    value = IntField()

    meta = {
        'ordering': ['-timestamp'],
        'indexes': [
            'search_term'
        ]
    }

    @staticmethod
    def get_latest_trend(search_term: str) -> 'Trend':
        """Returns the latest Trend object for the given search term.
        
        Arguments:
            search_term {str} -- The search term to lookup.
        
        Returns:
            Trend -- The latest associated Trend object.
        """
        return Trend.objects(search_term=search_term).first()

    @staticmethod
    def get_trends(search_term: str, is_chronological: bool = True) -> 'QuerySet[Trend]':
        """Returns all Trend data associated with the search term.
        
        Arguments:
            search_term {str} -- The search term to lookup.
            is_chronological {bool} -- Whether to return in chronological order. {default: True}
        
        Returns:
            QuerySet[Trend] -- An iterable QuerySet of the Trend data.
        """
        if is_chronological:
            return Trend.objects(search_term=search_term).order_by('timestamp')
        else:
            return Trend.objects(search_term=search_term)

    def as_dict(self) -> Dict:
        """Returns the data of the Trend object formatted as a Dict.
        
        Returns:
            Dict -- The Trend data as a dictionary.
        """
        return {
            "timestamp": str(self.get_timestamp()),
            "is_partial": self.get_is_partial(),
            "value": self.get_value()
        }

    def get_is_partial(self) -> bool:
        """Returns whether the Trend data is a partial result.
        
        Returns:
            bool -- True if it is a partial result; False otherwise.
        """
        return self.is_partial

    def get_search_term(self) -> str:
        """Returns the search term associated with the Trend object.
        
        Returns:
            str -- The search term.
        """
        return self.search_term

    def get_timestamp(self) -> datetime:
        """Returns the datetime of when the search Trend was recorded.
        
        Returns:
            datetime -- The time the search interest was measured by Google.
        """
        return self.timestamp

    def get_value(self) -> int:
        """Returns the relative value of the search interest. (0 to 100)
        
        Returns:
            int -- The relative value (0 to 100) of the Trend object.
        """
        return self.value
