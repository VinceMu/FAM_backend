from mongoengine import connect
import datafeed.assetclass as assetclass
import models as models
from datafeed.provider import *
import local_config, time, os, datetime

if os.environ['ALPHAVANTAGE_API_KEY'] == "[insert key here]":
    print('[DataLink] No connection established as API key missing.')
    exit(0)
is_running = True
asset_classes = []
provider = AlphaVantageProvider()
asset_classes.append(assetclass.CurrencyClass(provider))
asset_classes.append(assetclass.StocksClass(provider))
connect('FAM', host=local_config.MONGODB + "/" + local_config.DB)
for asset_class in asset_classes:
    print("[DataLink] Initiating " + asset_class.get_name() + " class")
    asset_class.on_startup()
while (is_running == True):
    update_start = datetime.datetime.now()
    print('[DataLink] Started update at ' + str(update_start))
    for asset_class in asset_classes:
        asset_class.on_interval()
        asset_class.on_daily()
    update_end = datetime.datetime.now()
    diff = round((update_end-update_start).total_seconds()/60)
    print('[DataLink] Finished update at ' + str(update_end) + ' taking ' + str(diff) + ' minutes')
    print('[DataLink] ... now idle for ' + str(local_config.REFRESH_INTERVAL) + ' seconds...')
    time.sleep(local_config.REFRESH_INTERVAL)
