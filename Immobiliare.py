import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pytz
import re
import main


url_by_table = [
        main.get_all_url_values("auction_properties", "immobiliare"),
        main.get_all_url_values("buildings", "immobiliare"),
        main.get_all_url_values("main_properties", "immobiliare")
    ]

properties_values_from_other_sites = [
    main.get_values_for_comparing_already_existing_props("auction_properties"),
    main.get_values_for_comparing_already_existing_props("buildings"),
    main.get_values_for_comparing_already_existing_props("main_properties")
]

GS_columns = ['Date_GMT', 'Titolo', 'Area', 'Link', 'Prezzo', 'Locali', 'MQ', 'Bagni','Piano', 'Descrizione', 'Dettagli', 'Agenzia']

DB_columns = ['Date_GMT', 'Titolo', 'Area', 'Link', 'Prezzo', 'Prezzo_al_mq','Locali', 'MQ', 'Bagni','Piano', 
              'Descrizione', 'Dettagli', 'Agenzia','contratto', 'anno di costruzione', 'stato',
              'riscaldamento','efficienza energetica', 'altre caratteristiche','Blueprint Url']

base_url = 'https://www.immobiliare.it/vendita-case/trieste-provincia/?criterio=rilevanza&pag={}'


def get_individual(url):
    req = requests.get(url).text
    soup = BeautifulSoup(req, 'html.parser')

    title = soup.find('div', class_='in-titleBlock__content').find('h1').text if soup.find('div', class_='in-titleBlock__content') else None
    title_text = title.split(',', 1) if title else [None]
    titolo = title_text[0].strip() if title_text[0] else None
    area = title_text[1].strip() if len(title_text) > 1 else None


    ul_tag = soup.find('ul', class_='nd-list nd-list--pipe in-feat in-feat--full in-feat__mainProperty in-landingDetail__mainFeatures')
    price_li = ul_tag.find('li', class_='nd-list__item in-feat__item in-feat__item--main in-detail__mainFeaturesPrice')
    price_li2 = ul_tag.find('li', class_='nd-list__item in-feat__item in-feat__item--main in-detail__mainFeaturesPrice in-detail__mainFeaturesPrice--interactive')



    locali_li = ul_tag.find(lambda tag: tag.name == 'li' and tag.get('aria-label') in ['locali', 'locale'])
    superficie_li = ul_tag.find('li', attrs={'aria-label': 'superficie'})
    bagni_li = ul_tag.find(lambda tag: tag.name == 'li' and tag.get('aria-label') in ['bagni', 'bagno'])
    piano_li = ul_tag.find('li', attrs={'aria-label': 'piano'})
    
    
    
    if piano_li and re.search(r'\d+', piano_li.text):
        piano = int(re.search(r'\d+', piano_li.text).group())
    else:
        dl_tag = soup.find('dl', class_='in-realEstateFeatures__list')
        dt_tags = dl_tag.find_all('dt', class_='in-realEstateFeatures__title')
        dd_tags = dl_tag.find_all('dd', class_='in-realEstateFeatures__value')
        
        
        for dt, dd in zip(dt_tags, dd_tags):
            if dt.text.strip() == 'piano' and re.search(r'\d+', dd.text):
                piano = int(re.search(r'\d+', dd.text).group())
                break
        else:
            
            for dt, dd in zip(dt_tags, dd_tags):
                if dt.text.strip() == 'totale piani edificio' and re.search(r'\d+', dd.text):
                    piano = int(re.search(r'\d+', dd.text).group())
                    break
            else:
                
                piano = None

    if price_li:
        price = price_li.text.strip().replace("€ ", "") 
    elif price_li2:
        price = price_li2.contents[0].strip().replace("€ ", "")
    else: 
        price = None
    
    
    locali = locali_li.text.strip() if locali_li else None
    superficie = superficie_li.text.strip().replace("m²", "") if superficie_li else None
    bagni = bagni_li.text.strip() if bagni_li else None


    
    dettagli_tag = soup.find('div', class_='in-readAll')
    dettagli = dettagli_tag.text.strip() if dettagli_tag else None
    descrizione = dettagli.split('.')[0].strip() if dettagli else None

    div_tag = soup.find('div', class_='in-referent in-referent__withPhone') or soup.find('div', class_='in-referent')
    agenzia = div_tag.find('p').text.strip() if div_tag and div_tag.find('p') else None

    DB_features = {}
    dl_tags = soup.find_all('dl', class_='in-realEstateFeatures__list')
    for dl_tag in dl_tags:
        if dl_tag:
            dt_tags = dl_tag.find_all('dt', class_='in-realEstateFeatures__title')
            dd_tags = dl_tag.find_all('dd', class_='in-realEstateFeatures__value')
            for dt, dd in zip(dt_tags, dd_tags):
                dt_text = dt.text.strip().lower()
                if dt_text == 'altre caratteristiche':
                    span_tags = dd.find_all('span', class_='in-realEstateFeatures__tag nd-tag')
                    dd_values = [span.text.strip() for span in span_tags]
                    DB_features[dt_text] = ', '.join(dd_values)
                elif dt_text == 'efficienza energetica':
                    energy_class = dd.find('span', class_='in-realEstateFeatures__energy').text.strip()
                    DB_features[dt_text] = energy_class
                else:
                    DB_features[dt_text] = dd.text.strip()





    
    unwanted_keys = ['superficie', 'locali', 'prezzo', 'spese condominio', 'cauzione','piano']
    DB_features = {k: v for k, v in DB_features.items() if k not in unwanted_keys}

    
    bp_tag = soup.find('div', class_='nd-figure__image nd-ratio nd-ratio--wide in-landingDetail__simpleGallery')
    img_tag = bp_tag.find('img', class_='nd-figure__content nd-ratio__img') if bp_tag else None

    
    DB_features['Blueprint Url'] = img_tag.get('src', 'Not available') if img_tag else 'Not available'


    
    extracted_datetime = datetime.now()

    
    gmt = pytz.timezone('GMT')

    
    extracted_datetime_gmt = extracted_datetime.astimezone(gmt)

    all_features = {}



    GS_features = {
        'Date_GMT': extracted_datetime_gmt.strftime('%Y-%m-%d %H:%M:%S %Z'),
        'Titolo': titolo,
        'Area':area,
        'Link': url,
        'Prezzo': price,
        'Locali': locali,
        'MQ': superficie,
        'Bagni': bagni,
        'Piano':piano,
        'Descrizione': descrizione,
        'Dettagli': dettagli,
        'Agenzia': agenzia,

    }

    all_features.update(GS_features)
    all_features.update(DB_features)

    return all_features

def get_all_properties():

    total_properties = 0
    total_pages = 0

    url = base_url.format(1)
    req = requests.get(url).text
    soup = BeautifulSoup(req, 'html.parser')

    total_properties = int(soup.find('div', class_='in-searchList__title').text.split()[0].replace('.', ''))
    total_pages = (total_properties + 24) // 25

    all_urls = []
    for page_number in range(1, total_pages + 1):
        url = base_url.format(page_number)
        req = requests.get(url).text
        soup = BeautifulSoup(req, 'html.parser')
        house_list = soup.find_all('div', class_='nd-mediaObject__content in-card__content in-realEstateListCard__content')

        for house in house_list:
            house_url = house.find('a')['href']
            all_urls.append(house_url)

    return all_urls

def compare_to_same_site(all_urls, url_by_table):

    not_on_db_urls = []

    for url in all_urls:

        is_on_db_imm = False

        for db_urls in url_by_table:

            is_on_db_imm = main.is_on_db_from_same_site(db_urls, url, "immobiliare")

            if is_on_db_imm:

                break

            else:

                pass

        if not is_on_db_imm:

            not_on_db_urls.append(url)
        

    return not_on_db_urls
        
def compare_to_other_site(partialy_scraped_properties):

    for values in properties_values_from_other_sites:

        is_on_db_other = main.is_on_db_from_another_site()

def update_delisted(urls_by_table, all_urls) -> list:

    all_new_props = []

    for db_table in urls_by_table:

        delisted_props, new_props = main.get_delisted_properties(db_table, all_urls, "immobiliare")

        all_new_props.append(new_props)

        for url in delisted_props:

            main.update_delisted_properties(db_table, "immobiliare", url)

    return all_new_props

def scrape_data(new_listed_props):

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_individual, url) for url in new_listed_props]

    details_list = [future.result() for future in futures]
    df = pd.DataFrame(details_list)

    return df



dataframe = scrape_data()


GS_df = dataframe[GS_columns]

DB_df = dataframe[DB_columns]


auction_df = dataframe[dataframe['Prezzo'].str.contains('da', na=False)]

auctionGS_df = auction_df[GS_columns]


DB_columns = ['Date_GMT', 'Titolo', 'Area', 'Link', 'Prezzo','Locali', 'MQ', 'Bagni','Piano', 
              'Descrizione', 'Dettagli', 'Agenzia','contratto', 'anno di costruzione', 'stato',
              'riscaldamento','efficienza energetica', 'altre caratteristiche','Blueprint Url']

auctionDB_df = auction_df[DB_columns]


building_df = dataframe[dataframe['Prezzo'].str.contains(r'\d+\s*-\s*\d+', na=False)]

buildingGS_df = building_df[GS_columns]

buildingDB_df = building_df[DB_columns]


buildingGS_df.head()


buildingDB_df.head()


main_df = dataframe[~dataframe['Prezzo'].str.contains(r'\d+\s*-\s*\d+|da', na=False)]
main_df.head()


main_df['Prezzo'] = pd.to_numeric(main_df['Prezzo'].replace(".", ""),errors = 'coerce')
main_df['MQ'] = pd.to_numeric(main_df['MQ'],errors = 'coerce')

main_df['Prezzo_al_mq'] = main_df['Prezzo']/main_df['MQ']

main_df.insert(5, 'Prezzo_al_mq', main_df.pop('Prezzo_al_mq'))

mainGS_df = main_df[GS_columns]

mainDB_df = main_df[DB_columns]