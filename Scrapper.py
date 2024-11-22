from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import json
from datetime import datetime, timedelta
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import os
import random
from fake_useragent import UserAgent
import requests
import threading
import pickle
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# Configuration des User Agents
USER_AGENTS = UserAgent()

# Ajouter ces constantes en haut du fichier
RETRY_DELAY = (400, 600)  # Délai en secondes avant de réessayer en cas de captcha
MAX_RETRIES = 3  # Nombre maximum de tentatives par URL
DELAY_BETWEEN_REQUESTS = (25, 45)  # Dlais un peu plus longs
MAX_CONCURRENT_SESSIONS = 3  # Augmenté de 3 à 5
SESSION_TIMEOUT = 150  # timeout en secondes
PROXY_BLACKLIST_DURATION = 1800  # 30 minutes en secondes


# Ajouter en haut du fichier après les imports
DESTINATIONS = {
    # Europe
    'LON': 'Londres',
    'MAD': 'Madrid',
    'BCN': 'Barcelone',
    'ROM': 'Rome',
    'AMS': 'Amsterdam',
    'BER': 'Berlin',
    'LIS': 'Lisbonne',
    'DUB': 'Dublin',
    'CPH': 'Copenhague',
    'VIE': 'Vienne',
    'PRG': 'Prague',
    'BRU': 'Bruxelles',
    'ATH': 'Athènes',
    'WAW': 'Varsovie',
    'BUD': 'Budapest',
    'ZRH': 'Zurich',
    'OSL': 'Oslo',
    'STO': 'Stockholm',
    'HEL': 'Helsinki',
    'IST': 'Istanbul',
    'MXP': 'Milan',
    
    # Amérique du Nord
    'NYC': 'New York',
    'LAX': 'Los Angeles',
    'SFO': 'San Francisco',
    'MIA': 'Miami',
    'CHI': 'Chicago',
    'YUL': 'Montréal',
    'YYZ': 'Toronto',
    'YVR': 'Vancouver',
    'MEX': 'Mexico',
    'CUN': 'Cancún',
    
    # Amérique du Sud
    'GRU': 'São Paulo',
    'EZE': 'Buenos Aires',
    'SCL': 'Santiago',
    'BOG': 'Bogota',
    'LIM': 'Lima',
    'RIO': 'Rio de Janeiro',
    
    # Asie
    'DXB': 'Dubai',
    'DOH': 'Doha',
    'AUH': 'Abu Dhabi',
    'SIN': 'Singapour',
    'HKG': 'Hong Kong',
    'BKK': 'Bangkok',
    'KUL': 'Kuala Lumpur',
    'NRT': 'Tokyo',
    'ICN': 'Séoul',
    'PEK': 'Pékin',
    'PVG': 'Shanghai',
    'DEL': 'New Delhi',
    'BOM': 'Mumbai',
    
    # Océanie
    'SYD': 'Sydney',
    'MEL': 'Melbourne',
    'AKL': 'Auckland',
    'BNE': 'Brisbane',
    'PER': 'Perth',
    
    # Afrique
    'JNB': 'Johannesburg',
    'CPT': 'Le Cap',
    'CAI': 'Le Caire',
    'CMN': 'Casablanca',
    'DKR': 'Dakar',
    'NBO': 'Nairobi'                                                                            
}
# Ajouter votre liste de proxies ici
CUSTOM_PROXIES = [
    "52.143.141.88:3128", 
    "4.178.185.235:3128",
    "20.199.91.99:3128",
    "20.199.94.172:3128",
    "4.251.124.194:3128",
    "4.251.123.247:3128",
    "4.178.175.105:3128",
    "4.212.8.170:3128",
    "4.251.113.113:3128",
    "4.212.15.184:3128",
    "4.251.116.117:3128",
    "4.211.104.4:3128",
    "4.211.105.36:3128",
    "4.211.105.71:3128",
    "4.178.189.175:3128",
    "4.178.189.213:3128",
    "4.178.189.160:3128",
    "4.251.113.80:3128"
]

def verify_proxy_list():
    print("Vérification des proxies...")
    working_proxies = []
    total = len(CUSTOM_PROXIES)
    
    for i, proxy in enumerate(CUSTOM_PROXIES, 1):
        print(f"\nTest du proxy {i}/{total}")
        if test_proxy(proxy):
            working_proxies.append(proxy)
    
    print(f"\nProxies fonctionnels trouvés : {len(working_proxies)}/{total}")
    return working_proxies

def test_proxy(proxy):
    """Teste si un proxy est fonctionnel"""
    print(f"Test du proxy : {proxy}")
    test_url = 'http://httpbin.org/ip'
    
    try:
        proxies = {
            'http': proxy,
        }
        
        # Configuration de la session avec des timeouts plus longs
        session = requests.Session()
        session.trust_env = False  # Ignorer les variables d'environnement proxy
        
        response = session.get(
            test_url,
            proxies=proxies,
            timeout=(10, 30),  # (connect timeout, read timeout)
            verify=False,
            headers={
                'User-Agent': USER_AGENTS.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive'
            },
            allow_redirects=True
        )
        
        if response.status_code == 200:
            print(f"✓ Proxy fonctionnel : {proxy}")
            return True
        else:
            print(f"✗ Proxy non fonctionnel (status code {response.status_code}): {proxy}")
            return False
            
    except requests.exceptions.ConnectTimeout:
        print(f"✗ Délai de connexion dépassé pour le proxy {proxy}")
        return False
    except requests.exceptions.ReadTimeout:
        print(f"✗ Délai de lecture dépassé pour le proxy {proxy}")
        return False
    except requests.exceptions.ProxyError as e:
        print(f"✗ Erreur de proxy pour {proxy}: {str(e)}")
        return False
    except Exception as e:
        print(f"✗ Erreur avec le proxy {proxy}: {str(e)}")
        return False
    finally:
        if 'session' in locals():
            session.close()

def cache_proxies(proxies):
    with open('proxy_cache.pkl', 'wb') as f:
        pickle.dump({
            'timestamp': datetime.now(),
            'proxies': proxies
        }, f)

def get_cached_proxies():
    try:
        with open('proxy_cache.pkl', 'rb') as f:
            data = pickle.load(f)
            if datetime.now() - data['timestamp'] < timedelta(hours=1):
                return data['proxies']
    except:
        pass
    return None

class ProxyRotator:
    def __init__(self):
        self.proxies = []
        self.current_index = 0
        self.lock = threading.Lock()
        self.last_refresh = datetime.now()
        self.refresh_interval = timedelta(minutes=30)
        self.blacklist = {}
        
        # Initialisation immédiate des proxies
        working_proxies = verify_proxy_list()
        self.proxies = working_proxies
    
    def blacklist_proxy(self, proxy):
        """Ajoute un proxy à la liste noire temporairement"""
        with self.lock:
            print(f"Ajout du proxy {proxy} à la liste noire")
            self.blacklist[proxy] = datetime.now()
            if proxy in self.proxies:
                self.proxies.remove(proxy)
                print(f"Proxy {proxy} retiré de la liste des proxies actifs")
    
    def get_next_proxy(self):
        """Retourne uniquement un proxy fonctionnel"""
        try:
            with self.lock:
                print("Obtention du prochain proxy...")
                
                # Nettoyer la liste noire des proxies expirés
                current_time = datetime.now()
                expired_proxies = [
                    proxy for proxy, blacklist_time in self.blacklist.items()
                    if (current_time - blacklist_time).total_seconds() > PROXY_BLACKLIST_DURATION
                ]
                
                # Réactiver les proxies expirés
                for proxy in expired_proxies:
                    if proxy not in self.proxies:
                        self.proxies.append(proxy)
                    del self.blacklist[proxy]
                
                if not self.proxies:
                    print("Aucun proxy disponible!")
                    return None
                
                # Sélection simple du prochain proxy
                proxy = self.proxies[self.current_index % len(self.proxies)]
                self.current_index = (self.current_index + 1) % len(self.proxies)
                
                print(f"Proxy sélectionné : {proxy}")
                return proxy
                
        except Exception as e:
            print(f"Erreur dans get_next_proxy: {str(e)}")
            return None

# Initialisation du rotateur de proxies
proxy_rotator = ProxyRotator()

@contextmanager
def create_driver():
    driver = None
    try:
        print("Initialisation du driver Chrome...")
        options = Options()
        
        # Headers fixes
        headers = {
            'User-Agent': 'Chrome/133.0.0.0',
            'Accept-Language': 'fr-FR',
            'Referer': 'https://www.google.com',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1'
        }
        
        # Configuration de base
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--start-minimized') 


        
        # Appliquer les headers fixes
        options.add_argument(f'user-agent={headers["User-Agent"]}')
        options.add_argument(f'--accept-language={headers["Accept-Language"]}')
        
        # Obtention du proxy uniquement
        proxy = proxy_rotator.get_next_proxy()
        
        if not proxy:
            raise RuntimeError("Impossible d'obtenir un proxy valide")

        # Configuration du proxy
        options.add_argument(f'--proxy-server={proxy}')
        
        # Désactiver la détection de l'automatisation
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        print(f"Configuration du proxy : {proxy}")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # Appliquer les headers via CDP
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": headers['User-Agent'],
            "acceptLanguage": headers['Accept-Language']
        })
        
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Configuration des timeouts
        driver.set_page_load_timeout(30)
        driver.set_script_timeout(30)
        
        print("Driver créé avec succès.")
        yield driver

    except Exception as e:
        print(f"Erreur détaillée lors de la création du driver : {str(e)}")
        if driver:
            try:
                driver.quit()
            except:
                pass
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def add_random_delays():
    """Ajoute des délais aléatoires pour simuler un comportement humain"""
    time.sleep(random.uniform(2, 5))

def simulate_human_behavior(driver):
    """Simule des comportements humains aléatoires"""
    try:
        # Scroll plus sûr
        driver.execute_script("window.scrollTo(0, Math.floor(Math.random() * 100));")
        time.sleep(random.uniform(1, 2))
        
        # Éviter les mouvements de souris qui peuvent causer des erreurs
        add_random_delays()
    except Exception as e:
        print(f"Erreur dans simulate_human_behavior: {str(e)}")
        pass

# Définir les sélecteurs en haut du fichier
SELECTORS = {
    # Mise à jour des sélecteurs existants et ajout de nouveaux
    'flight_card': "//div[contains(@class, 'nrc6')]",
    'times': ".//div[contains(@class, 'vmXl-mod-variant-large')]",
    'duration': ".//div[contains(@class, 'xdW8')]//div[contains(@class, 'vmXl')]",
    'price': ".//div[contains(@class, 'f8F1-price-text')]",
    'fare_class': "//div[contains(@class, 'nrc6-price-section')]//div[contains(@class, 'M_JD-large-display')]//div[contains(@class, 'DOum-name')]",
    'hand_baggage': ".//div[contains(@class, 'nrc6-price-section')]//div[contains(@class, 'Oihj-top-fees')]//div[contains(@class, 'ac27')]//div[1]//div[2]",
    'checked_baggage': ".//div[contains(@class, 'nrc6-price-section')]//div[contains(@class, 'Oihj-top-fees')]//div[contains(@class, 'ac27')]//div[2]//div[2]",
    'airlines': ".//div[contains(@class, 'c5iUd-leg-carrier')]//img",
    'layover': ".//div[contains(@class, 'JWEO-stops-text')]",
    'layover_info': """
        return document.querySelector("#listWrapper > div > div:nth-child(3) > div.Fxw9 > div:nth-child(" + arguments[0] + ") > div > div > div > div.nrc6-content-section > div.nrc6-main > div > ol > li > div > div > div.JWEO > div.c_cgF.c_cgF-mod-variant-full-airport > span > span")?.textContent || 'N/A';
    """,
    'airports': ".//span[contains(@class, 'jLhY-airport-info')]/span[1]",
}

def handle_cookie_popup(driver, wait):
    try:
        # Attendre que le bouton "Tout refuser" apparaisse et cliquer dessus
        reject_button = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//div[contains(@class, 'RxNS-button-content') and text()='Tout refuser']/ancestor::button")))
        driver.execute_script("arguments[0].click();", reject_button)
        print("Popup des cookies géré avec succès")
    except TimeoutException:
        print("Pas de popup de cookies trouvé")
    except Exception as e:
        print(f"Erreur lors de la gestion du popup des cookies : {str(e)}")

def scrape_kayak_flights(url):
    max_attempts = MAX_RETRIES
    attempt = 0
    current_proxy = None
    
    while attempt < max_attempts:
        try:
            print(f"\nTentative de scraping #{attempt + 1} pour {url}")
            
            if attempt > 0:
                delay = random.uniform(*RETRY_DELAY)
                print(f"Attente de {delay:.2f} secondes avant la nouvelle tentative...")
                time.sleep(delay)
            
            # Obtenir le proxy avant de créer le driver
            # Modification ici : on ne décompresse plus qu'une seule valeur
            current_proxy = proxy_rotator.get_next_proxy()
            
            if not current_proxy:
                print("Impossible d'obtenir un proxy valide")
                attempt += 1
                continue
            
            with create_driver() as driver:
                print("Driver créé, accès à l'URL...")
                driver.set_page_load_timeout(30)
                
                try:
                    driver.get(url)
                    print("Page chargée avec succès")
                except Exception as e:
                    print(f"Erreur lors du chargement de la page: {str(e)}")
                    if "captcha" in str(e).lower() or "timeout" in str(e).lower():
                        if current_proxy:
                            proxy_rotator.blacklist_proxy(current_proxy)
                        print("Captcha ou timeout détecté, changement de proxy...")
                        attempt += 1
                        continue
                    raise

                # Vérification de la présence de captcha dans le contenu
                if "captcha" in driver.page_source.lower() or "verify you're a human" in driver.page_source.lower():
                    if current_proxy:
                        proxy_rotator.blacklist_proxy(current_proxy)
                    print("Captcha détecté dans le contenu, changement de proxy...")
                    attempt += 1
                    continue

                # Ajouter des comportements humains aléatoires
                simulate_human_behavior(driver)
                
                # Créer un wait plus long pour les éléments critiques
                wait = WebDriverWait(driver, 15)
                short_wait = WebDriverWait(driver, 10)

                # Gérer le popup des cookies
                handle_cookie_popup(driver, wait)
                time.sleep(2)  # Attendre après la gestion des cookies

                # Vérifier si un captcha est présent
                try:
                    captcha_present = driver.find_element(By.CLASS_NAME, "WZTU-wrap")
                    if captcha_present:
                        print("Captcha détecté, changement de session...")
                        time.sleep(random.uniform(30, 60))
                        return scrape_kayak_flights(url)
                except:
                    pass

                # Attendre que les éléments principaux soient chargés avec des conditions explicites
                try:
                    wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "nrc6")))
                    wait.until(lambda driver: len(driver.find_elements(By.CLASS_NAME, "nrc6")) > 0)
                    wait.until(lambda driver: driver.find_element(By.CLASS_NAME, "nrc6").is_displayed())
                    
                    # Attendre spécifiquement que les horaires soient chargés
                    wait.until(EC.presence_of_element_located((By.XPATH, SELECTORS['times'])))
                    wait.until(lambda driver: driver.find_element(By.XPATH, SELECTORS['times']).text != '')
                except TimeoutException as e:
                    print(f"Timeout lors du chargement des éléments principaux : {str(e)}")
                    return None

                # Faire défiler la page avant de chercher le bouton
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
                time.sleep(2)

                # Cliquer sur "Plus de résultats" avec une attente conditionnelle
                try:
                    # Attendre que les résultats initiaux soient chargés
                    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "ULvh")))
                    time.sleep(2)  # Petit délai pour s'assurer que tout est chargé
                    
                    # Utiliser le sélecteur JavaScript exact
                    more_results_button = driver.execute_script(
                        'return document.querySelector("#listWrapper > div > div.ULvh > div")'
                    )
                    
                    if more_results_button:
                        print("Bouton 'Plus de résultats' trouvé")
                        
                        # Faire défiler jusqu'au bouton
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_results_button)
                        time.sleep(2)
                        
                        # Tenter le clic avec JavaScript
                        driver.execute_script("arguments[0].click();", more_results_button)
                        print("Clic sur 'Plus de résultats' effectué")
                        
                        # Attendre que les nouveaux résultats soient chargés
                        time.sleep(3)
                        wait.until(lambda driver: len(driver.find_elements(By.CLASS_NAME, "nrc6")) > 0)
                    else:
                        print("Bouton 'Plus de résultats' non trouvé")
                        
                except Exception as e:
                    print(f"Erreur lors du clic sur 'Plus de résultats': {str(e)}")

                # Attendre un peu plus longtemps pour s'assurer que tout est chargé
                time.sleep(2)

                print("Analyse du contenu de la page...")
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                flights = []
                
                # Limiter à 20 résultats
                flight_cards = soup.find_all('div', {'class': 'nrc6'})[:25]
                print(f"Nombre de cartes de vol à analyser : {len(flight_cards)}")
                
                # Récupérer les headers actuels
                headers = {
                    'User-Agent': 'Chrome/133.0.0.0',
                    'Accept-Language': 'fr-FR',
                    'Referer': 'https://www.google.com',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'same-origin',
                    'Sec-Fetch-User': '?1'
                }
                
                for index, card in enumerate(flight_cards):
                    try:
                        print(f"\nAnalyse du vol {index + 1}...")
                        
                        # Extraction des horaires et détection des escales
                        times = card.find_all('div', {'class': 'vmXl'})
                        departure_time = times[0].text if times else "N/A"
                        
                        # Vérification si c'est un vol avec escale
                        is_direct = "escale" not in (times[1].text if len(times) > 1 else "")
                        
                        # Si c'est un vol avec escale, chercher l'heure d'arrivée réelle
                        if not is_direct:
                            try:
                                arrival_time = driver.find_element(
                                    By.XPATH, 
                                    f"(//div[contains(@class, 'nrc6')])[{index + 1}]//div[contains(@class, 'vmXl')][last()]"
                                ).text
                            except:
                                arrival_time = "N/A"
                        else:
                            arrival_time = times[1].text if len(times) > 1 else "N/A"

                        # Extraction de la durée
                        duration = "N/A"
                        try:
                            duration_element = card.find('div', {'class': 'xdW8'})
                            duration = duration_element.text if duration_element else "N/A"
                        except:
                            pass

                        # Extraction du prix
                        price = "N/A"
                        try:
                            price_element = card.find('div', {'class': 'f8F1'})
                            price = price_element.text if price_element else "N/A"
                        except:
                            pass

                        # Extraction de la classe tarifaire avec JavaScript
                        fare_class_script = """
                            return Array.from(document.querySelectorAll("#listWrapper > div > div:nth-child(3) > div.Fxw9 > div:nth-child(" + arguments[0] + ") > div > div > div > div.nrc6-price-section > div > div.Oihj-bottom-booking > div > div.M_JD-large-display > div:nth-child(2) > div > div > div > div > div"))
                            .map(el => el.textContent.trim())
                            .filter(text => text !== '')[0] || 'N/A';
                        """
                        fare_class = driver.execute_script(fare_class_script, index + 1)
                        
                        # Si JavaScript échoue, essayer avec XPath comme fallback
                        if fare_class == 'N/A':
                            fare_class_element = driver.find_element(
                                By.XPATH,
                                SELECTORS['fare_class']
                            )
                            fare_class = fare_class_element.text.strip() if fare_class_element else "N/A"
                            
                        # Nettoyage et normalisation de la classe tarifaire
                        fare_class = fare_class.replace('Tarif ', '').strip()
                        
                        # Mapping des classes tarifaires pour normalisation
                        fare_class_mapping = {
                            'LIGHT': 'Light',
                            'STANDARD': 'Standard',
                            'FLEX': 'Flex',
                            'BUSINESS': 'Business',
                            'PREMIÈRE': 'First',
                            'BASIC': 'Basic',           # Ajout de Basic
                            'ÉCONOMIQUE': 'Economy',    # Ajout de Économique -> Economy
                            'ECONOMIQUE': 'Economy'     # Pour gérer le cas sans accent
                        }
                        
                        fare_class = fare_class_mapping.get(fare_class.upper(), fare_class)
                        
                        # Nouvelle logique pour les bagages
                        try:
                            # Attendre que les éléments de bagages soient chargés
                            wait = WebDriverWait(driver, 2)
                            
                            # Bagage à main
                            hand_baggage_element = wait.until(
                                EC.presence_of_element_located((
                                    By.XPATH,
                                    f"(//div[contains(@class, 'nrc6')])[{index + 1}]//div[contains(@class, 'nrc6-price-section')]//div[contains(@class, 'Oihj-top-fees')]//div[contains(@class, 'ac27')]//div[1]//div[2]"
                                ))
                            )
                            # Petit délai pour s'assurer que le texte est chargé
                            time.sleep(0.5)
                            hand_baggage = hand_baggage_element.text.strip() if hand_baggage_element else "N/A"
                            
                            # Bagage en soute
                            checked_baggage_element = wait.until(
                                EC.presence_of_element_located((
                                    By.XPATH,
                                    f"(//div[contains(@class, 'nrc6')])[{index + 1}]//div[contains(@class, 'nrc6-price-section')]//div[contains(@class, 'Oihj-top-fees')]//div[contains(@class, 'ac27')]//div[2]//div[2]"
                                ))
                            )
                            # Petit délai pour s'assurer que le texte est chargé
                            time.sleep(0.5)
                            checked_baggage = checked_baggage_element.text.strip() if checked_baggage_element else "N/A"
                            
                            # Vérification supplémentaire pour s'assurer que les valeurs ne sont pas vides
                            if not hand_baggage or hand_baggage.isspace():
                                hand_baggage = "N/A"
                            if not checked_baggage or checked_baggage.isspace():
                                checked_baggage = "N/A"
                                
                        except TimeoutException:
                            print(f"Timeout lors de l'extraction des bagages pour le vol {index + 1}")
                            hand_baggage = "N/A"
                            checked_baggage = "N/A"
                        except Exception as e:
                            print(f"Erreur lors de l'extraction des bagages pour le vol {index + 1}: {str(e)}")
                            hand_baggage = "N/A"
                            checked_baggage = "N/A"

                        # Extraction de la destination
                        try:
                            # Utiliser BeautifulSoup pour trouver tous les éléments d'aéroport
                            airport_elements = card.find_all('span', {'class': 'jLhY-airport-info'})
                            if airport_elements:
                                # Prendre le dernier élément (destination) et extraire le premier span
                                last_airport = airport_elements[-1]
                                destination_airport = last_airport.find('span').text.strip()
                            else:
                                destination_airport = "N/A"
                        except Exception as e:
                            print(f"Erreur lors de l'extraction de la destination : {str(e)}")
                            destination_airport = "N/A"

                        # Extraction des compagnies aériennes
                        try:
                            airline_elements = driver.find_elements(
                                By.XPATH,
                                f"(//div[contains(@class, 'nrc6')])[{index + 1}]//div[contains(@class, 'c5iUd-leg-carrier')]//img"
                            )
                            airlines = [element.get_attribute('alt') for element in airline_elements if element.get_attribute('alt')]
                            
                            if not airlines:  # Backup method using BeautifulSoup
                                airline_elements = card.find_all('div', {'class': 'c5iUd-leg-carrier'})
                                airlines = [img.get('alt') for element in airline_elements if element.find('img') for img in [element.find('img')] if img and img.get('alt')]
                            
                            if not airlines:
                                airlines = ["N/A"]
                        except Exception as e:
                            print(f"Erreur lors de l'extraction des compagnies aériennes : {str(e)}")
                            airlines = ["N/A"]

                        # Initialisation des variables d'escale
                        layover_info = "N/A"
                        layover_duration = "N/A"

                        # Si ce n'est pas un vol direct, essayer d'extraire les informations d'escale
                        if not is_direct:
                            try:
                                # Utiliser le nouveau sélecteur JavaScript pour l'escale
                                layover_info = driver.execute_script(SELECTORS['layover_info'], index + 1)
                                
                                if layover_info != 'N/A':
                                    # Essayer d'obtenir la durée d'escale via l'attribut title
                                    layover_duration_element = driver.execute_script("""
                                        return document.querySelector("#listWrapper > div > div:nth-child(3) > div.Fxw9 > div:nth-child(" + arguments[0] + ") > div > div > div > div.nrc6-content-section > div.nrc6-main > div > ol > li > div > div > div.JWEO > div.c_cgF.c_cgF-mod-variant-full-airport > span > span")?.getAttribute('title') || 'N/A';
                                    """, index + 1)
                                    
                                    if layover_duration_element != 'N/A':
                                        # Nettoyer la durée d'escale
                                        layover_duration = layover_duration_element.split('escale de ')[1].split(' à')[0] if 'escale de ' in layover_duration_element else layover_duration_element
                                    else:
                                        layover_duration = "N/A"
                                else:
                                    # Fallback vers l'ancien sélecteur XPath si nécessaire
                                    try:
                                        layover_element = driver.find_element(
                                            By.XPATH, 
                                            f"(//div[contains(@class, 'nrc6')])[{index + 1}]//div[contains(@class, 'JWEO')]//div[contains(@class, 'c_cgF')]//span/span"
                                        )
                                        if layover_element:
                                            layover_info = layover_element.text
                                            layover_duration = layover_element.get_attribute('title')
                                            if layover_duration:
                                                layover_duration = layover_duration.split('escale de ')[1].split(' à')[0]
                                    except:
                                        layover_info = "N/A"
                                        layover_duration = "N/A"
                            except Exception as e:
                                print(f"Erreur lors de l'extraction de l'escale : {str(e)}")
                                layover_info = "N/A"
                                layover_duration = "N/A"

                        flight = {
                            "departure_time": departure_time,
                            "arrival_time": arrival_time,
                            "duration": duration,
                            "price": price,
                            "origin_airport": "BOD",
                            "destination_airport": destination_airport,
                            "is_direct": is_direct,
                            "checked_baggage": checked_baggage,
                            "hand_baggage": hand_baggage,
                            "layover_airport": layover_info,
                            "layover_duration": layover_duration,
                            "airlines": airlines,
                            "fare_class": fare_class
                        }
                        
                        if not all(v == "N/A" for v in flight.values()):
                            flights.append(flight)
                            print(f"Vol ajouté avec succès: {flight}")
                        else:
                            print("Vol ignoré car toutes les valeurs sont N/A")
                            
                    except Exception as e:
                        print(f"Erreur lors de l'extraction du vol {index + 1}: {str(e)}")
                        continue
                
                print(f"\nNombre total de vols extraits : {len(flights)}")
                return {
                    "flights": flights,
                    "headers_used": headers,
                    "proxy_used": proxy_rotator.get_next_proxy()
                }
                
        except Exception as e:
            print(f"Erreur détaillée lors de la tentative #{attempt + 1}: {str(e)}")
            if "captcha" in str(e).lower():
                if current_proxy:
                    proxy_rotator.blacklist_proxy(current_proxy)
                print("Captcha détecté, changement de proxy...")
                attempt += 1
                continue
            attempt += 1
            continue
            
        break
    
    if attempt >= max_attempts:
        print(f"Échec après {max_attempts} tentatives")
        return None

def generate_dates(start_date_str, end_date_str=None, num_days=None):
    """Genère une liste de dates entre start_date et end_date (ou pour num_days)"""
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    
    if end_date_str:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        delta = (end_date - start_date).days + 1
        return [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(delta)]
    else:
        num_days = num_days or 30  # utilise 30 jours par défaut si non spécifié
        return [(start_date + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(num_days)]


def get_kayak_url(date, destination):
    """Génère l'URL Kayak pour une date et une destination données"""
    return f"https://www.kayak.fr/flights/BOD-{destination}/{date}?sort=bestflight_a"

# Ajouter cette fonction pour nettoyer les fichiers temporaires
def cleanup_chrome_files():
    import shutil
    import os
    
    try:
        temp_dir = os.path.expanduser('~\\appdata\\roaming\\undetected_chromedriver')
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    except Exception as e:
        print(f"Erreur lors du nettoyage des fichiers temporaires : {str(e)}")

def cleanup_driver_files():
    import shutil
    import os
    
    # Nettoyer le dossier .wdm s'il existe
    wdm_path = os.path.join(os.path.expanduser('~'), '.wdm')
    if os.path.exists(wdm_path):
        try:
            shutil.rmtree(wdm_path)
        except Exception as e:
            print(f"Erreur lors du nettoyage de .wdm: {str(e)}")
    
    # Nettoyer le dossier local drivers s'il existe
    local_drivers = os.path.join(os.getcwd(), 'drivers')
    if os.path.exists(local_drivers):
        try:
            shutil.rmtree(local_drivers)
        except Exception as e:
            print(f"Erreur lors du nettoyage du dossier drivers: {str(e)}")

# Ajouter cette fonction pour une installation manuelle si nécessaire
def manual_chromedriver_setup():
    print("""
    Si l'installation automatique ne fonctionne pas, suivez ces étapes :
    
    1. Désactivez temporairement Windows Defender
    2. Téléchargez manuellement ChromeDriver depuis : https://chromedriver.chromium.org/downloads
    3. Créez un dossier 'drivers' dans votre projet
    4. Copiez chromedriver.exe dans ce dossier
    5. Ajoutez une exclusion dans Windows Defender pour ce dossier
    6. Modifiez le chemin dans le code pour pointer vers ce dossier
    """)

# Ajouter après la fonction scrape_kayak_flights et avant la fonction main

def process_destination_with_semaphore(destination):
    """Traite une destination donnée avec un sémaphore pour limiter les connexions simultanées"""
    try:
        print(f"\nScraping des vols pour {DESTINATIONS[destination]}")
        
        # Créer le dossier pour la destination
        destination_dir = os.path.join("data", destination)
        os.makedirs(destination_dir, exist_ok=True)
        
        # Générer les dates
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = "2025-05-01"
        dates = generate_dates(start_date, end_date)
        
        for date in dates:
            try:
                # Construire le nom du fichier
                filename = os.path.join(destination_dir, f"flights_{date}.json")
                
                # Vérifier si le fichier existe déjà
                if os.path.exists(filename):
                    print(f"Données déjà existantes pour {destination} le {date}")
                    continue
                
                # Construire l'URL et scraper les données
                url = get_kayak_url(date, destination)
                print(f"\nScraping des vols pour {DESTINATIONS[destination]} le {date}")
                
                results = scrape_kayak_flights(url)
                
                if results and results.get('flights'):
                    # Créer le dictionnaire final avec les métadonnées
                    final_data = {
                        "search_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "flight_date": date,
                        "origin": "BOD",
                        "destination": destination,
                        "destination_city": DESTINATIONS[destination],
                        "url": url,
                        "flights": results['flights']
                    }
                    
                    # Sauvegarder les résultats
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(final_data, f, ensure_ascii=False, indent=4)
                    print(f"Données sauvegardées pour {destination} le {date}")
                else:
                    print(f"Pas de résultats pour {destination} le {date}")
                
                # Attendre entre chaque requête
                time.sleep(random.uniform(*DELAY_BETWEEN_REQUESTS))
                
            except Exception as e:
                print(f"Erreur lors du traitement de {destination} pour la date {date}: {str(e)}")
                continue
                
    except Exception as e:
        print(f"Erreur lors du traitement de la destination {destination}: {str(e)}")
        raise

# Modifier la fonction main pour inclure la concaténation
def main():
    try:
        print("Démarrage du script...")
        
        # Test initial avec timeout
        print("Test initial du driver...")
        timeout = time.time() + 30  # 30 secondes maximum pour le test
        test_success = False
        
        while not test_success and time.time() < timeout:
            try:
                with create_driver() as test_driver:
                    test_driver.get("https://www.google.com")
                    test_success = True
                    print("Test initial du driver réussi!")
            except Exception as e:
                print(f"Tentative de test échouée: {str(e)}")
                time.sleep(2)
        
        if not test_success:
            raise RuntimeError("Impossible de créer un driver fonctionnel après plusieurs tentatives")

        # Configuration initiale
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = "2025-06-30"
        dates = generate_dates(start_date, end_date)
        max_workers = 10  # Augmenté de 2 à 4 workers
        
        print(f"Dates générées: {len(dates)} jours")
        print(f"Nombre de workers: {max_workers}")
        
        # Créer un dossier drivers local
        os.makedirs("drivers", exist_ok=True)
        
        # Créer le dossier principal pour les données
        os.makedirs("data", exist_ok=True)
        
        # Exécuter le scraping pour chaque destination en parallèle
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_destination = {executor.submit(process_destination_with_semaphore, dest): dest 
                                   for dest in DESTINATIONS.keys()}
            
            for future in concurrent.futures.as_completed(future_to_destination):
                destination = future_to_destination[future]
                try:
                    future.result()
                    print(f"Scraping terminé pour {DESTINATIONS[destination]}")
                except Exception as e:
                    print(f"Erreur pour {DESTINATIONS[destination]}: {str(e)}")

        print("\nScraping terminé pour toutes les destinations")
        

    except Exception as e:
        if "chromedriver" in str(e).lower():
            print("Problème avec ChromeDriver détecté.")
            manual_chromedriver_setup()
            return
        raise

# Ajouter une fonction pour vérifier si le système est prêt
def check_system_ready():
    try:
        # Nettoyer les dossiers temporaires existants
        cleanup_chrome_files()
        
        # Vérifier l'espace disque disponible
        import psutil
        disk = psutil.disk_usage('/')
        if disk.percent > 90:
            print("Attention: Espace disque faible")
            return False
            
        return True
    except Exception as e:
        print(f"Erreur lors de la vérification du système : {str(e)}")
        return False

# Modifier le point d'entrée principal
if __name__ == "__main__":
    if check_system_ready():
        try:
            main()
        finally:
            cleanup_chrome_files()
    else:
        print("Le système n'est pas prêt pour l'exécution")
