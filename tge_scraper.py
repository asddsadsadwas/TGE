import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import os
import time

def get_polish_date():
    """
    Get current date in Polish timezone (Warsaw, UTC+1/UTC+2).
    Returns date in DD-MM-YYYY format.
    """
    try:
        from zoneinfo import ZoneInfo
        polish_time = datetime.now(ZoneInfo("Europe/Warsaw"))
    except (ImportError, Exception):
        try:
            import pytz
            warsaw_tz = pytz.timezone('Europe/Warsaw')
            polish_time = datetime.now(warsaw_tz)
        except ImportError:
            utc_now = datetime.utcnow()
            import calendar
            year = utc_now.year
            
            last_sunday_march = max(week[-1] for week in calendar.monthcalendar(year, 3))
            last_sunday_october = max(week[-1] for week in calendar.monthcalendar(year, 10))
            
            dst_start_utc = datetime(year, 3, last_sunday_march, 1, 0, 0)
            dst_end_utc = datetime(year, 10, last_sunday_october, 1, 0, 0)
            
            if dst_start_utc <= utc_now < dst_end_utc:
                polish_offset = timedelta(hours=2)
            else:
                polish_offset = timedelta(hours=1)
            
            polish_time = utc_now + polish_offset
    
    return polish_time.strftime("%d-%m-%Y")

COLUMN_MAPPING = {
    0: "Instrument",
    1: "Typ instrumentu",
    2: "Fixing I - Kurs [PLN/MWh]",
    3: "Fixing I - Wolumen [MW]",
    4: "Fixing II - Kurs jednolity [PLN/MWh]",
    5: "Fixing II - Wolumen [MW]",
    6: "Notowania ciągłe - Kurs jednolity [EUR/MWh]",
    7: "Notowania ciągłe - Kurs jednolity [PLN/MWh]",
    8: "Notowania ciągłe - Wolumen [MW]",
    9: "Notowania ciągłe - Wolumen kupna [MW]",
    10: "Notowania ciągłe - Wolumen sprzedaży [MW]",
    11: "Łącznie - Kurs min. [PLN/MWh]",
    12: "Łącznie - Kurs max. [PLN/MWh]",
    13: "Łącznie - Kurs średnioważony [PLN/MWh]",
    14: "Łącznie - Wolumen [MWh]",
    15: "Łącznie - Wolumen kupna [MWh]",
    16: "Łącznie - Wolumen sprzedaży [MWh]"
}

def scrape_tge_data(date_str=None, max_retries=3):
    """
    Scrape TGE electricity market data and filter for instrument type 60.
    
    Args:
        date_str: Date in format DD-MM-YYYY (if None, uses today's Polish date)
        max_retries: Maximum number of retry attempts
    """
    if date_str is None:
        date_str = get_polish_date()
        print(f"Używam dzisiejszej daty (czas Polski): {date_str}")
    
    url = f"https://tge.pl/energia-elektryczna-rdn?dateShow={date_str}&dateAction=prev"
    
    print(f"Pobieranie danych z: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt
                print(f"Próba {attempt + 1}/{max_retries} po {wait_time}s...")
                time.sleep(wait_time)
            
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            table = soup.find('table', {'id': 'rdn'})
            if not table:
                print("Nie znaleziono tabeli RDN na stronie.")
                if attempt < max_retries - 1:
                    continue
                return None
            
            rows = []
            tbody = table.find('tbody')
            if tbody:
                for tr in tbody.find_all('tr'):
                    cells = tr.find_all('td')
                    if cells:
                        row_dict = {}
                        for idx, cell in enumerate(cells):
                            col_name = cell.get('data-title') or cell.get('data-label')
                            if not col_name:
                                col_name = COLUMN_MAPPING.get(idx, f"Kolumna_{idx + 1}")
                            row_dict[col_name] = cell.get_text(strip=True)
                        rows.append(row_dict)
            
            if not rows:
                print("Nie znaleziono żadnych danych w tabeli.")
                return None
            
            df = pd.DataFrame(rows)
            
            print(f"Znalezione kolumny ({len(df.columns)}): {list(df.columns)}")
            
            print(f"\nPobrano {len(df)} wierszy z tabeli.")
            
            if 'Typ instrumentu' in df.columns:
                instrument_col = 'Typ instrumentu'
            else:
                print(f"UWAGA: Nie znaleziono kolumny 'Typ instrumentu'")
                print(f"Dostępne kolumny: {list(df.columns)}")
                return df
            
            if instrument_col:
                filtered_df = df[df[instrument_col] == '60']
                print(f"Znaleziono {len(filtered_df)} wierszy z Typ instrumentu = 60")
                return filtered_df
            else:
                print(f"UWAGA: Nie znaleziono kolumny 'Typ instrumentu' ani 'Kolumna_2'")
                print(f"Dostępne kolumny: {list(df.columns)}")
                return df
                
        except requests.exceptions.RequestException as e:
            print(f"Błąd podczas pobierania danych (próba {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return None
        except Exception as e:
            print(f"Błąd podczas przetwarzania danych: {e}")
            return None
    
    return None

def save_to_file(df, filename="tge_data.csv"):
    """
    Save DataFrame to CSV file.
    
    Args:
        df: pandas DataFrame with data
        filename: name of the output file
    """
    if df is None or df.empty:
        print("Brak danych do zapisania.")
        return
    
    try:
        desktop_path = os.path.expanduser("~\Desktop")
        if not os.path.exists(desktop_path):
            desktop_path = "."
        
        filepath = os.path.join(desktop_path, filename)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"\nDane zapisane do pliku: {filepath}")
        
    except Exception as e:
        print(f"Błąd podczas zapisywania pliku: {e}")

def display_data(df):
    """
    Display DataFrame in console.
    
    Args:
        df: pandas DataFrame with data
    """
    if df is None or df.empty:
        print("Brak danych do wyświetlenia.")
        return
    
    print("\n" + "="*80)
    print("WYNIKI - Typ instrumentu = 60")
    print("="*80)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    
    print(df.to_string(index=False))
    print("\n" + "="*80)

if __name__ == "__main__":
    print("TGE Web Scraper - Energia Elektryczna RDN")
    print("=" * 50)
    
    data = scrape_tge_data()
    
    if data is not None and not data.empty:
        display_data(data)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tge_data_{timestamp}.csv"
        save_to_file(data, filename)
    else:
        print("\nNie udało się pobrać danych.")
