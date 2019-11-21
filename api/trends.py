from flask_restplus import abort, Namespace, Resource
from flask import jsonify, make_response, Response
from flask_jwt_extended import jwt_required
from pytrends.request import TrendReq
import json

API = Namespace('trends', description='Trends Endpoint')


@API.route("/<string:keyword>")
class Trends(Resource):

    @jwt_required
    def get(self,keyword):
        """
        :param keyword: keyword to search by in our case asset name.
        :return: see Sample Response
        """
        if keyword is None or keyword == "":
            return None, 404
        trend_request = TrendReq()
        trend_request.build_payload([keyword])
        trends_over_time = trend_request.interest_over_time()
        json_obj = json.loads(trends_over_time.to_json(orient="index",date_format="iso"))
        formatted_json = [{"time_stamp":time_stamp,
                           "value": json_obj[time_stamp][keyword],
                           "isPartial":json_obj[time_stamp]["isPartial"]}
                          for time_stamp,_ in json_obj.items()]
        return make_response(jsonify(formatted_json),200)
    """
    :returns
    Sample Response:
    [
    {
        "isPartial": "False",
        "time_stamp": "2014-11-23T00:00:00.000Z",
        "value": 40
    },
    {
        "isPartial": "False",
        "time_stamp": "2014-11-30T00:00:00.000Z",
        "value": 33
    },
    {
        "isPartial": "False",
        "time_stamp": "2014-12-07T00:00:00.000Z",
        "value": 33
    },
    {
        "isPartial": "False",
        "time_stamp": "2014-12-14T00:00:00.000Z",
        "value": 33
    },
    ]
    """
