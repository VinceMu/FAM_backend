from datetime import datetime
import os
import time

from mongoengine import connect

import datafeed.assetclass as AssetClass
from datafeed.provider import AlphaVantageProvider
import local_config as CONFIG

class DataLink:
    def __init__(self):
        self.asset_classes = []
        self.is_running = True

    def load_asset_classes(self) -> None:
        """Loads all the asset classes in the DataLink.
        """
        provider = AlphaVantageProvider()
        self.asset_classes.append(AssetClass.CurrencyClass(provider))
        self.asset_classes.append(AssetClass.StockClass(provider))

    def run(self) -> None:
        """Begins running the DataLink which involves:
        - Loading asset classes
        - Calling startup methods
        - Checking for updates on asset classes periodically
        """
        if os.environ['ALPHAVANTAGE_API_KEY'] == CONFIG.DEFAULT_KEY:
            CONFIG.DATA_LOGGER.error("No connection established as API key missing")
            return
        print("[DataLink] Loading asset classes")
        self.load_asset_classes()
        for asset_class in self.asset_classes:
            asset_class.on_startup()
        print("[DataLink] ===> Done")
        while self.is_running:
            update_start = datetime.now()
            print("[DataLink] Started update check at " + str(update_start))
            for asset_class in self.asset_classes:
                asset_class.on_interval()
            update_end = datetime.now()
            diff = (update_end-update_start).total_seconds()
            diff_minutes = int(diff/60)
            diff_seconds = round(diff-(diff_minutes*60))
            print("[DataLink] Finished update at " + str(update_end) + " taking " + str(diff_minutes) + "m:" + str(diff_seconds) + "s")
            time.sleep(CONFIG.REFRESH_INTERVAL)

connect('FAM', host=CONFIG.MONGODB + "/" + CONFIG.DB)
LINK = DataLink()
LINK.run()
print("[DataLink] Shutting down...")
