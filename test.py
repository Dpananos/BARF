import requests
import logging

class APIScraper:

    def __init__(self, url):

        self.url = url

    def fetch(self, params):
        response = requests.get(self.url, params=params)
        return response

    def save_data(self, response, filename):

        if response.status_code == 200:
            with open(filename, 'w') as file:
                file.write(response.text)

            logging.info(f"Data saved to {filename}")

        else: 
            logging.error(f"Failed to fetch data: {response.status_code} and message: {response.text}")



