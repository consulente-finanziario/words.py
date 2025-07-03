import requests
import xml.etree.ElementTree as ET
from collections import Counter
from bs4 import BeautifulSoup
import logging
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_urls_from_sitemap(sitemap_url, session):
    """
    Scarica e analizza una sitemap XML, restituendo la lista delle URL.
    """
    try:
        resp = session.get(sitemap_url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Errore download sitemap {sitemap_url}: {e}")
        return []

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logging.error(f"Errore parsing XML sitemap {sitemap_url}: {e}")
        return []

    urls = [loc.text.strip() for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc') if loc.text]
    logging.info(f"Trovate {len(urls)} URL in {sitemap_url}")
    return urls


def process_url(url, session):
    """
    Scarica il contenuto della pagina e conta parole che:
      - iniziano con lettera maiuscola;
      - lunghe almeno 3 lettere;
      - non sono la prima parola di una frase.
    """
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.warning(f"Fallito download {url}: {e}")
        return Counter()

    soup = BeautifulSoup(resp.text, 'html.parser')
    for tag in soup(['script', 'style']):
        tag.extract()
    text = soup.get_text(separator=' ')

    # Suddividi il testo in frasi basate su punteggiatura
    sentences = re.split(r'(?<=[.!?])\s+', text)
    counter = Counter()

    # Per ogni frase, conta solo le parole dalla seconda in poi
    for sentence in sentences:
        # Trova parole capitalizzate di almeno 3 caratteri
        words = re.findall(r"\b[A-Z][a-zA-Z]{2,}\b", sentence)
        # Conta solo dalla seconda parola in poi per evitare maiuscole iniziali
        for word in words[1:]:
            counter[word] += 1

    return counter


def main():
    # Configurazione logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    sitemap_urls = [
        'https://consulente-finanziario.org/post-sitemap.xml',
        'https://consulente-finanziario.org/post-sitemap2.xml'
    ]

    with requests.Session() as session:
        # Estrai URL dalle sitemap
        all_urls = []
        for sm in sitemap_urls:
            all_urls.extend(get_urls_from_sitemap(sm, session))

        total = len(all_urls)
        logging.info(f"Inizio scansione di {total} pagine con ThreadPool...")

        total_counter = Counter()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(process_url, url, session): url for url in all_urls}
            for idx, future in enumerate(as_completed(futures), 1):
                url = futures[future]
                try:
                    total_counter.update(future.result())
                except Exception as e:
                    logging.error(f"Errore processing {url}: {e}")
                percent = (idx / total) * 100
                logging.info(f"Pagina {idx}/{total} ({percent:.1f}%) processata: {url}")

    # Scrivi risultati su file filtrando ancora eventuali rimanenze
    output_file = 'word_counts.txt'
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for word, count in total_counter.most_common():
                f.write(f"{word}\t{count}\n")
        logging.info(f"Risultati salvati in {output_file}")
    except IOError as e:
        logging.error(f"Errore scrittura file {output_file}: {e}")
        sys.exit(1)

    # Mantieni il programma in esecuzione finché non premi invio
    input("Scansione completata. Premi invio per uscire...")

if __name__ == '__main__':
    main()
