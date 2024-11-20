import re
import bs4
import random
import aiohttp
import asyncio
import jsonlines
import requests
from aiohttp import ClientSession


class GoldAppleParser():
    def __init__(self, page_product_count=24):
        self.page_product_count = page_product_count
        self.urls = {
            'https://goldapple.ru/uhod': 53000, #53к
            'https://goldapple.ru/volosy': 21000, # 21к
            'https://goldapple.ru/odezhda-i-aksessuary': 18000, #18к
            'https://goldapple.ru/makijazh': 17000, #17к
            'https://goldapple.ru/aptechnaja-kosmetika': 16000, #16к
            'https://goldapple.ru/azija': 15000, #15к
            'https://goldapple.ru/parfjumerija': 13000, #13к
            'https://goldapple.ru/organika': 13000, #13к
            'https://goldapple.ru/figura-mechty': 12000, #12к
            'https://goldapple.ru/dlja-muzhchin': 10000, #10к
            'https://goldapple.ru/ukrashenija': 9000, #9к
            'https://goldapple.ru/novye-brendy': 5000, #5к
            'https://goldapple.ru/lajm': 6000, #6к
            # 'https://goldapple.ru/dlja-doma': None, #
            'https://goldapple.ru/mini-formaty': 5000, #5к
            'https://goldapple.ru/uborki-i-gigiena': 4000, #4к
            'https://goldapple.ru/detjam': 4000, #4к
            'https://goldapple.ru/tehnika': 3000, #3к
            'https://goldapple.ru/aptechnaja-kosmetika/bady': 3000, #3к
            'https://goldapple.ru/sexual-wellness': 2000, #2к
        }


    async def get_product_count(self, session: ClientSession, url: str) -> None:
        '''
        Функция, предназначенная для нахождения количества товаров по каждому разделу сайта.
        '''
        async with session.get(url) as response:
            html = await response.text()
            bs = bs4.BeautifulSoup(html, 'html.parser')
            try:
                self.urls[url] = int(bs.find('span', class_='NZnR3')['data-category-products-count'])
            except TypeError:
                pass


    async def run_product(self) -> None:
        ''''
        Данная функция запускает вышеуказанную функцию.
        '''
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                self.get_product_count(session, url)
                for url in self.urls.keys()
            ])

    

    async def fetch(self, session: ClientSession, url: str) -> None:
        '''
        Ассинхронная функция для пасринга сайта.
        Записывает данные, полученные с конкртеной страницы, в файл.
        '''

        async with session.get(url) as response:
            try:
                html = await response.text()
                bs = bs4.BeautifulSoup(html, 'html.parser')
                products = bs.find_all('a', class_='TTjPn Drqwh QUKnI')
                for product in products:
                    href = product['href']
                    id_ = href[1:href.find('-')]
                    name = product.find('span', class_='KkVNn').text.strip()
                    brand = product.find('span', class_='Padcv').text.strip()
                    type_ = product.find('div', class_='_7uTPQ').text.strip()
                    photo = product.find('img')['src']
                    price_1 = int(
                        re.sub(
                            '\D+',
                            '',
                            product.find('div', class_='+XURy').text
                        )
                    )
                    price_2 = int(product.find_all('meta')[-3]['content'])
                    in_stock = True if (
                        product.find_all('meta')[-1]['itemprop'] == 'availability'
                    ) else False
                    with jsonlines.open('output.jsonl', mode='a') as writer:
                        writer.write(
                            {   
                                'id': id_,
                                'name': name,
                                'brand': brand,
                                'type': type_,
                                'photo': photo,
                                'price': price_1,
                                'in_stock': in_stock
                            }
                        )
            except Exception as e:
                with open('errors.txt', mode='a') as file:
                    file.write(f'{e}\n')
        
    
    async def main(self) -> None:
        '''
        Главная функция класса, запускающая парсинг.
        '''
        urls = []

        for url, total_product_count in self.urls.items():
            urls.extend(
                [f'{url}?p={url_num}' for url_num in range(2, total_product_count // self.page_product_count)]
            )
        

        random.shuffle(urls)
        
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                self.fetch(session, url)
                for url in urls
            ])

if __name__ == 'main':
    gap = GoldAppleParser(24)
    asyncio.run(gap.main())
