from mongoengine import connect
import datafeed.assetclass as assetclass
import models as models
from datafeed.provider import *
import local_config, time, os

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
    for asset_class in asset_classes:
        asset_class.on_interval()
        asset_class.on_daily()
    print('[DataLink] ... now idle for ' + str(local_config.REFRESH_INTERVAL) + ' seconds...')
    time.sleep(local_config.REFRESH_INTERVAL)
