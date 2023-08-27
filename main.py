from bs4 import BeautifulSoup
import requests
import pandas as pd
import pymysql


def get_top_url(url):
    req = requests.get(url)
    assert req.status_code == 200, 'Hay un problema con la pagina de inicio'

    soup = BeautifulSoup(req.text, 'lxml')
    top_url = url + soup.find('a', attrs={'class': 'seeMore'}).get('href')

    return top_url


def get_best_weekly_books(url, top_100):
    req = requests.get(top_100)
    assert req.status_code == 200, 'Hay un problema con la pagina del top'

    soup = BeautifulSoup(req.text, 'lxml')
    secciones = soup.find('div', attrs={'class': 'content'}).find_all('article')
    libros = [url + seccion.a.get('href') for seccion in secciones]

    return libros


def get_data(libros):
    libro_c = 0

    titulos = []
    precio = []
    precio_usd = []
    errores = []

    for libro in libros:
        req = requests.get(libro)

        try:
            assert req.status_code == 200, f'Hay un problema con el libro del indice {libro_c}'
        except AssertionError:
            errores.append(libro_c)

        soup = BeautifulSoup(req.text, 'lxml')

        try:
            titulos.append(soup.find('h1', attrs={'itemprop': 'name'}).find('a').get('title'))
            precio.append(float(
                soup.find('div', attrs={'class': 'column-left'}).find_all('div')[-2].text.replace('AR$ ', '').replace(
                    '.', '').replace(',', '.')))
            precio_usd.append(float(
                soup.find('div', attrs={'class': 'column-left'}).find_all('div')[-1].text.replace('U$s ', '').replace(
                    ',', '.')))
        except AttributeError:
            print(f'Hubo un problema con el libro del indice {libro_c}')
            errores.append(libro_c)
        finally:
            libro_c += 1

    return titulos, precio, precio_usd, errores


def get_usd_blue_price():
    url = 'https://dolarhoy.com/cotizaciondolarblue'
    req = requests.get(url)
    assert req.status_code == 200, 'Hay un problema con la cotizacion del dolar blue'

    soup = BeautifulSoup(req.text, 'lxml')
    valor_dolar = soup.find_all('div', attrs={'class': 'value'})[1].text
    valor_dolar = float(valor_dolar.replace('$', ''))

    return valor_dolar


def organized_data(titles, books_links, prices, usd_prices, blue_prices):
    lista = []

    for n in range(len(titles)):
        aux_lista = [titles[n], books_links[n], prices[n], usd_prices[n], blue_prices[n]]
        lista.append(aux_lista)

    return lista


"""def get_errors(errors, titles, books_links):
    lista = []

    if errors != lista:

        for n in errors:
            try:
                lista.append([errors[n], [titles[n], books_links[n]]])
            except IndexError:
                print('Hubo un problema con el libro del indice {n}')

    return lista"""


def main(url):
    top_100 = get_top_url(url)

    books_links = get_best_weekly_books(url, top_100)

    titles, prices, usd_prices, errors = get_data(books_links)

#    errors = get_errors(errors, titles, books_links)

    blue_usd_price = get_usd_blue_price()
    blue_prices = [n / blue_usd_price for n in prices]

    values = organized_data(titles, books_links, prices, usd_prices, blue_prices)

    upload_data(values, errors)


def upload_data(values, errors):
    data = open('utilities.txt', mode='r')

    connection = pymysql.connect(

        host=data.readline().replace('\n', ''),
        database=data.readline().replace('\n', ''),
        user=data.readline().replace('\n', ''),
        password=data.readline().replace('\n', '')

    )

    cursor = connection.cursor()

    cursor.execute('TRUNCATE TABLE top_100')
    connection.commit()

    cursor.executemany('INSERT INTO top_100(title, link, price, usd_price, blue_price, date)'
                       'VALUES(%s,%s,%s,%s,%s,now())', values)
    #    cursor.executemany('INSERT INTO errors()')
    connection.commit()


if __name__ == "__main__":
    url = 'https://www.cuspide.com'
    main(url)
