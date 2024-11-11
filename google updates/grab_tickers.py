from typing import List
import pandas as pd
import logging
import gspread
import yaml
import time

class GrabTickers:
    def __init__(self, config_path: str = "dev/config.yaml"):
        # Start Logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.config_path = config_path
        self.worksheet_name: str = ""
        self.sheet_name: str = ""
        self.refresh_rate: int
        self.rate_limit: int
        self.credentials_path: str = ""
        self.ticker_header: str = ""
        self.tickers: List[str] = []

        # Load Configuration
        self.load_config()

    def load_config(self) -> None:
        """Load configuration from YAML file with error handling and logging."""
        try:
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
                self.worksheet_name = config.get("worksheet_name", "")
                self.sheet_name = config.get("sheet_name", "")
                self.refresh_rate = config.get("refresh_rate", 60) # default
                self.rate_limit = config.get("rate_limit", 300) # default for google sheets quota is 300/m
                self.credentials_path = config.get("credentials_path", "")
                self.ticker_header = config.get("tickers_header_name", "tickers")

                # Log each configuration variable
                logging.info(f"Loaded configuration: worksheet_name={self.worksheet_name}, "
                             f"sheet_name={self.sheet_name}, refresh_rate={self.refresh_rate}, "
                             f"rate_limit={self.rate_limit}, credentials_path={self.credentials_path}, "
                             f"ticker_header={self.ticker_header}")

                # Validate essential config parameters
                if not all([self.worksheet_name, self.sheet_name, self.credentials_path, self.ticker_header]):
                    raise ValueError("Some configuration parameters are missing or invalid.")

        except FileNotFoundError:
            logging.error(f"Configuration file not found at path: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML config: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error loading config: {e}")
            raise

    def grab_tickers(self) -> None:
        """Authenticate with Google Sheets and load ticker data."""
        try:
            gc = gspread.service_account(filename=self.credentials_path)
            sheet = gc.open(self.worksheet_name).worksheet(self.sheet_name)

            # Load data into DataFrame
            all_values = sheet.get_all_values()
            self.df_less_headers = pd.DataFrame(all_values[1:], columns=all_values[0])
            self.tickers = self.df_less_headers[self.ticker_header].dropna().tolist()
            logging.info(f"Successfully grabbed {len(self.tickers)} tickers.")

        except gspread.exceptions.GSpreadException as e:
            logging.error(f"Google Sheets API error: {e}")
            raise
        except FileNotFoundError:
            logging.error(f"Credentials file not found at path: {self.credentials_path}")
            raise
        except KeyError:
            logging.error(f"Ticker header '{self.ticker_header}' not found in sheet columns.")
            raise
        except Exception as e:
            logging.error(f"Unexpected error grabbing tickers: {e}")
            raise

    def main(self) -> None:
        """Main loop to periodically fetch and process tickers."""
        while True:
            start_time = time.time()
            try:
                self.grab_tickers()

                elapsed_time = time.time() - start_time
                logging.info(f"Iteration completed in {elapsed_time:.2f} seconds.")
                time.sleep(max(0, self.refresh_rate - elapsed_time))

            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}")

if __name__ == "__main__":
    updater = GrabTickers()
    updater.main()
