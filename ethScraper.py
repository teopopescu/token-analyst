import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import urllib


class ethScraper():

    @staticmethod
    def extract_classifications():
        url = 'https://etherscan.io/labelcloud'
        response = requests.get(url)
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        soup.find_all('button')
        buttons = soup.find_all('button')
        button_text = []
        for button in buttons:
            button_text.append(button.get_text().split("(", 1)[0][2:].strip())
        del button_text[-2:]
        del button_text[:2]
        return button_text

    @staticmethod
    def extract_accounts_data(classification_name):
        accounts_url = 'https://etherscan.io/accounts?l='
        encoded_url = urllib.parse.quote(classification_name)
        accounts_url += encoded_url
        response = requests.get(accounts_url)
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        cells = soup.find_all('td')
        if len(cells) > 1:
            cell_text = []
            for cell in cells:
                cell_text.append(cell.get_text())
            addresses = cell_text[0::4]
            labels = cell_text[1::4]
            etherscan_data = pd.DataFrame({'Address': addresses, 'Labels': labels})
            etherscan_data['Classification'] = classification_name
            etherscan_data['Source'] = 'Etherscan'
            etherscan_data['Type'] = 'Account'
        else:
            etherscan_data = pd.DataFrame([['No address', 'No labels', classification_name, 'Etherscan', 'Account']],
                                          columns=['Address', 'Labels', 'Classification', 'Source', 'Type'])
        return etherscan_data


    @staticmethod
    def extract_tokens_data(classification_name):
        accounts_url = 'https://etherscan.io/tokens?l='
        encoded_url = urllib.parse.quote(classification_name)
        accounts_url += encoded_url
        response = requests.get(accounts_url)
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        cells = soup.find_all('td')
        if len(cells) > 1:
            cell_text = []
            for cell in cells:
                cell_text.append(cell.get_text())
            addresses = cell_text[0::4]
            labels = cell_text[1::4]
            etherscan_data = pd.DataFrame({'Contract': addresses, 'TokenName': labels})
            etherscan_data['Classification'] = classification_name
            etherscan_data['Source'] = 'Etherscan'
            etherscan_data['Type'] = 'Token'
        else:
            etherscan_data = pd.DataFrame([['No contract', 'No token name', classification_name, 'Etherscan', 'Token']],
                                          columns=['Contract', 'TokenName', 'Classification', 'Source', 'Type'])
        return etherscan_data




if __name__  ==  "__main__":
    classification = ethScraper.extract_classifications()

    ethScraper.extract_accounts_data(classification[0]).to_csv('accounts_data.csv', index=False)
    with open('accounts_data.csv', 'a') as f:
        i = 0
        while i < len(classification[1:]):
            extracted_data = ethScraper.extract_accounts_data(classification[1:][i])
            if extracted_data['Address'].empty:
               i+=1
            else:
                extracted_data.to_csv(f, header=False,index=False)
                i+=1

    ethScraper.extract_tokens_data(classification[0]).to_csv('tokens_data.csv', index=False)
    with open('tokens_data.csv', 'a') as f:
        # for item in classification[1:]:
        i = 0
        while i < len(classification[1:]):
            # extracted_data = ethScraper.extract_accounts_data(item)
            extracted_data = ethScraper.extract_tokens_data(classification[1:][i])
            if extracted_data['Contract'].empty:
                i += 1
            else:
                extracted_data.to_csv(f, header=False,index=False)
                i += 1