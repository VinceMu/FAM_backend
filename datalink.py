from mongoengine import connect
import datafeed.assetclass as AssetClass
import local_config as CONFIG
from datafeed.provider import AlphaVantageProvider
import local_config, time, os, datetime

class DataLink:
    def __init__(self):
        self.asset_classes = []
        self.is_running = True

    def load_asset_classes(self):
        provider = AlphaVantageProvider()
        self.asset_classes.append(AssetClass.CurrencyClass(provider))
        self.asset_classes.append(AssetClass.StockClass(provider))

    def run(self):
        if os.environ['ALPHAVANTAGE_API_KEY'] == "[insert key here]":
            CONFIG.DATA_LOGGER.error("No connection established as API key missing")
            return
        self.load_asset_classes()
        for asset_class in self.asset_classes:
            asset_class.on_startup()
        while self.is_running:
            update_start = datetime.datetime.now()
            CONFIG.DATA_LOGGER.info("Started update check at " + str(update_start))
            for asset_class in self.asset_classes:
                asset_class.on_interval()
            update_end = datetime.datetime.now()
            diff = round((update_end-update_start).total_seconds()/60)
            CONFIG.DATA_LOGGER.info("Finished update at " + str(update_end) + " taking " + str(diff) + " minutes")
            time.sleep(CONFIG.REFRESH_INTERVAL)

connect('FAM', host=CONFIG.MONGODB + "/" + CONFIG.DB)
LINK = DataLink()
LINK.run()
CONFIG.DATA_LOGGER.info("Program has terminated")

