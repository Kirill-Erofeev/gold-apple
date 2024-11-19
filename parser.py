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
        async with session.get(url) as response:
            html = await response.text()
            bs = bs4.BeautifulSoup(html, 'html.parser')
            try:
                self.urls[url] = int(bs.find('span', class_='NZnR3')['data-category-products-count'])
            except TypeError:
                pass
        # for k, v in self.urls.items():
        #     print(k, '    ', v)


    async def run_product(self) -> None:
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
                # if bs.find('title').text == 'упс... страница не найдена':
                #     print('exit')
                products = bs.find_all('a', class_='TTjPn Drqwh QUKnI')
                for product in products:
                    href = product['href']
                    id_ = href[1:href.find('-')]
                    name = product.find('span', class_='KkVNn').text.strip()
                    brand = product.find('span', class_='Padcv').text.strip()
                    type_ = product.find('div', class_='_7uTPQ').text.strip()
                    # photos = list(map(lambda x: x['srcset'], product.find_all('source')))
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
                    # if price_1 == price_2:
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

        # for url in self.urls:
        #     urls.extend([f'{url}?p={url_num}' for url_num in range(100)])
        for url, total_product_count in self.urls.items():
            urls.extend(
                [f'{url}?p={url_num}' for url_num in range(2, total_product_count // self.page_product_count)]
            )
        
        # urls = ['https://goldapple.ru/lajm?p=5', 'https://goldapple.ru/aptechnaja-kosmetika?p=5']

        # random.shuffle(urls)
        # urls = urls[:6000]
        # print(urls[:11])
        
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                self.fetch(session, url)
                for url in urls
            ])

            # for page_num in range(1, 3):
                # async with session.get(f'https://goldapple.ru/lajm/uhod?p={page_num}') as response:



gap = GoldAppleParser(24)
# asyncio.run(gap.run_product())
asyncio.run(gap.main())

