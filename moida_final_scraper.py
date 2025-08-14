#!/usr/bin/env python3
"""
Batched Moida Skincare Scraper

This scraper works in batches of 10 products per category, with a maximum of 60 products total.
It tracks previously scraped products to avoid duplicates on subsequent runs.
"""

import requests
import json
import time
import re
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import Dict, List, Optional
import logging
from bs4 import BeautifulSoup
from decimal import Decimal, InvalidOperation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BatchedMoidaScraper:
    """Batched scraper for Moida skincare products with progress tracking"""
    
    def __init__(self):
        self.base_url = "https://moidaus.com"
        # self.skincare_url is not used for scraping; collections come from Moida/scrape.txt
        self.skincare_url = "https://moidaus.com/collections/skin-care"
        self.session = requests.Session()
        # Optional: file with comma-separated collection URLs to scrape
        self.collections_file = os.path.join('Moida', 'scrape.txt')
        
        # Ethical scraping headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://moidaus.com/',
        })
        
        # Rate limiting settings
        self.delay_between_requests = 3  # seconds (more conservative for ethical scraping)
        self.max_retries = 3
        
        # Batch settings
        self.batch_size = 10  # products per category
        self.max_total_products = 60  # maximum products per run
        
        # Progress tracking
        self.progress_file = "scraping_progress.json"
        self.output_file = "output_moida_batched.json"
        self.scraped_urls = set()
        self.load_progress()
        # Remember last set of source collection URLs used in a run for metadata
        self.last_source_urls: List[str] = []
        
        # Known brand prefixes for extraction from product titles
        # Extend this list over time as needed
        self.known_brands = [
            'COSRX', 'AXIS-Y', 'Axis-Y', 'Tonymoly', 'Beauty of Joseon', 'SKIN1004', 'Skinfood', 'Anua', 'Tocobo',
            'Beauty Of Joseon', 'Beauty Of JOSEON'
        ]
        
    def load_progress(self):
        """Load previously scraped URLs to avoid duplicates"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress_data = json.load(f)
                    self.scraped_urls = set(progress_data.get('scraped_urls', []))
                    logger.info(f"Loaded {len(self.scraped_urls)} previously scraped URLs")
            else:
                logger.info("No previous progress found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading progress: {e}")
            self.scraped_urls = set()
    
    def save_progress(self):
        """Save progress to avoid scraping the same products again"""
        try:
            progress_data = {
                'scraped_urls': list(self.scraped_urls),
                'last_updated': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved progress with {len(self.scraped_urls)} scraped URLs")
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def make_request(self, url: str) -> Optional[requests.Response]:
        """Make a request with proper error handling and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Making request to: {url}")
                response = self.session.get(url, timeout=20)
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:  # Rate limited
                    wait_time = (attempt + 1) * 15
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Request failed with status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay_between_requests)
                    
        return None

    def canonicalize_product_url(self, url: str) -> str:
        """Return absolute canonical product URL (strip query/fragment)"""
        if not url:
            return ''
        if not url.startswith('http'):
            url = urljoin(self.base_url, url)
        parsed = urlparse(url)
        canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return canonical

    def fetch_product_json(self, product_url: str) -> Optional[Dict]:
        """Fetch Shopify product JSON for richer data (images/vendor/variants)"""
        try:
            canonical_url = self.canonicalize_product_url(product_url)
            # Shopify exposes product JSON at product handle with .json
            if not canonical_url:
                return None
            json_url = canonical_url.rstrip('/') + '.json'
            time.sleep(self.delay_between_requests)
            response = self.make_request(json_url)
            if not response or response.status_code != 200:
                return None
            data = response.json()
            # Some themes wrap as { product: {...} }
            if isinstance(data, dict) and 'product' in data:
                return data['product']
            return data if isinstance(data, dict) else None
        except Exception as e:
            logger.warning(f"Failed to fetch product JSON for {product_url}: {e}")
            return None

    def normalize_price(self, raw_price: Optional[str]) -> str:
        """Normalize a Shopify price into $xx.xx string using Decimal."""
        if raw_price is None:
            return ''
        try:
            if isinstance(raw_price, (int, float)):
                value = Decimal(str(raw_price))
            else:
                raw_str = str(raw_price).strip().replace('$', '')
                if raw_str.isdigit() and len(raw_str) >= 3:
                    value = (Decimal(raw_str) / Decimal(100))
                else:
                    value = Decimal(raw_str.replace(',', ''))
            return f"${value:.2f}"
        except (InvalidOperation, ValueError):
            return ''

    def _remove_noise_tags(self, soup: BeautifulSoup) -> None:
        """Remove tags that introduce noise (script/style/noscript/template/svg). Mutates soup."""
        for tag_name in ['script', 'style', 'noscript', 'template', 'svg']:
            for t in soup.find_all(tag_name):
                t.decompose()

    def _sanitize_text(self, text: str) -> str:
        """Collapse whitespace and trim common disclaimers/artifacts from a text blob."""
        if not text:
            return ''
        t = text
        # Trim wishlist/app artifacts if any leaked
        t = re.sub(r"frcp\.[\s\S]*$", "", t, flags=re.IGNORECASE)
        # Truncate at disclaimers frequently present after ingredients
        t = re.split(r"ingredients\s+subject\s+to\s+change[\s\S]*?$", t, flags=re.IGNORECASE)[0]
        t = re.split(r"for the most complete[\s\S]*?list of ingredients[\s\S]*?$", t, flags=re.IGNORECASE)[0]
        t = re.split(r"subject to change[\s\S]*?packaging[\s\S]*?$", t, flags=re.IGNORECASE)[0]
        # Collapse whitespace
        t = re.sub(r"\s+", " ", t).strip()
        return t

    def extract_price_from_json_ld(self, soup: BeautifulSoup) -> str:
        """Try to read Product.offer price from JSON-LD structured data."""
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or script.text or '{}')
            except Exception:
                continue
            # Sometimes it's a list
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue
                if (obj.get('@type') == 'Product') or ('Product' in obj.get('@type', [])):
                    offers = obj.get('offers')
                    if isinstance(offers, list):
                        prices = []
                        for off in offers:
                            p = off.get('price') if isinstance(off, dict) else None
                            if p:
                                norm = self.normalize_price(p)
                                if norm:
                                    prices.append(Decimal(norm.replace('$', '')))
                        if prices:
                            return f"${min(prices):.2f}"
                    elif isinstance(offers, dict):
                        p = offers.get('price') or offers.get('lowPrice')
                        if p:
                            norm = self.normalize_price(p)
                            if norm:
                                return norm
        return ''

    def extract_ingredients_from_page(self, soup: BeautifulSoup) -> str:
        """Extract Ingredients section from cleaned product page soup with quality checks."""
        # Heuristic quality check: require commas or semicolons indicating list
        def looks_like_ingredients(s: str) -> bool:
            s_norm = s.lower()
            return (s.count(',') >= 3) or (s.count(';') >= 2) or bool(re.search(r"\([^)]+\)", s_norm))

        # 1) Look for a heading-like node with exact text 'Ingredients'
        for tag_name in ['h1', 'h2', 'h3', 'h4', 'strong', 'b', 'dt', 'p', 'span']:
            for tag in soup.find_all(tag_name):
                label = (tag.get_text(strip=True) or '').strip()
                if re.fullmatch(r"(?i)ingredients", label):
                    # Prefer next sibling or definition description
                    # dt/dd case
                    if tag_name == 'dt':
                        dd = tag.find_next_sibling('dd')
                        if dd:
                            txt = self._sanitize_text(dd.get_text(separator=' ', strip=True))
                            if looks_like_ingredients(txt):
                                return txt
                    # General siblings until next heading
                    texts = []
                    for sib in tag.next_siblings:
                        if getattr(sib, 'name', '') in ['h1', 'h2', 'h3', 'h4', 'strong', 'b', 'dt']:
                            break
                        if getattr(sib, 'get_text', None):
                            piece = sib.get_text(separator=' ', strip=True)
                        else:
                            piece = str(sib).strip()
                        piece = self._sanitize_text(piece)
                        if piece:
                            texts.append(piece)
                    candidate = self._sanitize_text(' '.join(texts))
                    if looks_like_ingredients(candidate):
                        return candidate

        # 2) Look for inline label like 'Ingredients: <text>' inside paragraphs/sections
        for container in soup.select('div, section, article, p, li'):
            raw = container.get_text(separator=' ', strip=True)
            if not raw:
                continue
            m = re.search(r"(?i)\bingredients\b\s*[:\-]?\s*(.+)$", raw)
            if m:
                txt = self._sanitize_text(m.group(1))
                if looks_like_ingredients(txt):
                    return txt

        return ''

    def extract_ingredients_from_body_html(self, body_html: str) -> str:
        """Parse Shopify body_html to extract Ingredients section when available"""
        try:
            if not body_html:
                return ''
            soup = BeautifulSoup(body_html, 'html.parser')
            def clean_text(t: str) -> str:
                if not t:
                    return ''
                # Truncate at common disclaimer
                t = re.split(r'subject to change.*?packaging', t, flags=re.IGNORECASE)[0]
                t = re.split(r'subject to change', t, flags=re.IGNORECASE)[0]
                t = re.split(r'for the most complete.*?list of ingredients', t, flags=re.IGNORECASE)[0]
                # Remove obvious script/app artifacts
                lines = [ln for ln in t.splitlines() if not re.search(r'frcp\.|wishlist|modalJsUrl|Shopify|function\(|\{\}', ln, re.IGNORECASE)]
                t = ' '.join(lines)
                return t.strip()
            # Look for any text block that contains 'Ingredients' and then capture following content
            # 1) Look for headings
            for heading_tag in ['h1', 'h2', 'h3', 'h4', 'strong', 'b']:
                for tag in soup.find_all(heading_tag):
                    text_val = tag.get_text(strip=True)
                    if re.fullmatch(r'(?i)ingredients', text_val):
                        # Collect next siblings text as ingredients
                        texts = []
                        for sib in tag.next_siblings:
                            if getattr(sib, 'get_text', None):
                                text = sib.get_text(separator=' ', strip=True)
                            else:
                                text = str(sib).strip()
                            if text:
                                texts.append(text)
                            # Stop if another heading encountered
                            if getattr(sib, 'name', '') in ['h1', 'h2', 'h3', 'h4', 'strong', 'b']:
                                break
                        ingredients_text = clean_text(' '.join(texts).strip())
                        if ingredients_text:
                            return ingredients_text
            # 2) Fallback: regex split after 'Ingredients'
            plain = clean_text(soup.get_text(separator=' ', strip=True))
            split = re.split(r'(?i)\bingredients\b\s*[:\-]?\s*', plain)
            if len(split) > 1:
                return split[1].strip()
            return ''
        except Exception:
            return ''

    def derive_brand_from_title(self, title: str) -> str:
        """Try to derive brand by matching known brand names at the start of title"""
        if not title:
            return ''
        normalized = title.strip()
        # Strip leading promotional tags like *DEAL*, *SPECIAL PRICE*, *CLEARANCE*, etc.
        normalized = re.sub(r"^\s*(\*[^*]*\*\s*)+", "", normalized)
        # If title starts with [Brand], prefer the bracket content
        bracket_match = re.match(r"^\s*\[\s*([^\]]+?)\s*\]\s*", normalized)
        if bracket_match:
            return bracket_match.group(1).strip()
        # If any bracketed word appears early, treat as brand
        bracket_any = re.search(r"\[\s*([^\]]+?)\s*\]", normalized)
        if bracket_any:
            return bracket_any.group(1).strip()
        # Remove leading punctuation that may precede the brand
        normalized = re.sub(r"^[\-*_\s]+", "", normalized)
        for brand in self.known_brands:
            if normalized.lower().startswith(brand.lower()):
                return brand
        # If no known brand matched, use the first token as a last-resort heuristic
        # but only if it looks like an all-caps word or a proper noun
        first_tokens = normalized.split()
        token = first_tokens[0] if first_tokens else ''
        token = token.strip('[](){}')
        return token
    
    def discover_products_from_collection_page(self, collection_url: str) -> List[Dict]:
        """Discover products from a collection page URL"""
        logger.info(f"Discovering products from collection: {collection_url}")
        
        response = self.make_request(collection_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        products = []

        # Derive category from the collection URL
        category = self.derive_category_from_collection_url(collection_url)
        
        # Look for product containers (based on analysis)
        product_containers = soup.find_all(['div', 'article'], class_=lambda x: x and any(word in x.lower() for word in ['product', 'item', 'card', 'grid']))
        
        logger.info(f"Found {len(product_containers)} product containers")
        
        for container in product_containers:
            try:
                # Extract product link
                product_link = container.find('a', href=True)
                if not product_link:
                    continue
                    
                href = product_link.get('href')
                if not href or '/products/' not in href:
                    continue
                # Canonicalize to avoid variant duplicates
                canonical_href = self.canonicalize_product_url(href)
                
                # Skip if already scraped
                if canonical_href in self.scraped_urls:
                    logger.info(f"Skipping already scraped product: {canonical_href}")
                    continue
                
                # Extract product name
                name_elem = container.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']) or container.find(class_=lambda x: x and any(word in x.lower() for word in ['title', 'name', 'product']))
                product_name = name_elem.get_text().strip() if name_elem else "Unknown Product"
                
                # Extract price
                price_elem = container.find(string=re.compile(r'\$'))
                price = price_elem.strip() if price_elem else ""
                
                # Do not trust list-page images; prefer product page/gallery
                image_url = ""
                
                # Extract vendor/brand
                vendor_elem = container.find(string=re.compile(r'Vendor:', re.IGNORECASE))
                vendor = vendor_elem.strip() if vendor_elem else ""
                
                product_info = {
                    'name': product_name,
                    'url': canonical_href,
                    'price': price,
                    'image_url': image_url,
                    'vendor': vendor,
                    'category': category,
                    'brand': ''
                }
                
                products.append(product_info)
                logger.info(f"Found product: {product_name} - {href}")
                
            except Exception as e:
                logger.error(f"Error extracting product from container: {e}")
                continue
        
        # Remove duplicates based on URL
        unique_products = []
        seen_urls = set()
        for product in products:
            if product['url'] not in seen_urls:
                unique_products.append(product)
                seen_urls.add(product['url'])
        
        logger.info(f"Discovered {len(unique_products)} unique products from {collection_url}")
        return unique_products

    def load_collection_urls(self, path: Optional[str] = None) -> List[str]:
        """Load collection URLs from a comma-separated txt file. Falls back to default skincare URL if missing."""
        file_path = path or self.collections_file
        urls: List[str] = []
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                # Split by comma or newline
                raw_parts = re.split(r"[,\n]", content)
                for part in raw_parts:
                    u = part.strip()
                    if not u:
                        continue
                    # Normalize to absolute URL if needed
                    if not u.startswith('http'):
                        u = urljoin(self.base_url, u)
                    urls.append(u)
            else:
                logger.warning(f"Collections file not found: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to load collection URLs from {file_path}: {e}")
        return urls

    def derive_category_from_collection_url(self, url: str) -> str:
        """Infer category from a collection URL, e.g., /collections/mask -> 'mask',
        strip trailing numeric suffixes like '-1', and replace hyphens with spaces."""
        try:
            path = urlparse(url).path
            # Expect /collections/<slug>
            parts = [p for p in path.split('/') if p]
            if 'collections' in parts:
                idx = parts.index('collections')
                if idx + 1 < len(parts):
                    slug = parts[idx + 1]
                else:
                    slug = ''
            else:
                slug = parts[-1] if parts else ''
            # remove trailing -digits
            slug = re.sub(r"-\d+$", "", slug)
            # map hyphens to spaces
            category = slug.replace('-', ' ').strip()
            return category or 'unknown'
        except Exception:
            return 'unknown'
    
    def extract_image_from_product_page(self, product_url: str, product_name: str) -> str:
        """Extract main image URL from individual product page (prefer product JSON)"""
        logger.info(f"Extracting image from: {product_url}")
        # Try product JSON first for accurate images
        product_json = self.fetch_product_json(product_url)
        if product_json and isinstance(product_json, dict):
            images = product_json.get('images') or []
            if images and isinstance(images, list):
                first = images[0]
                if isinstance(first, dict) and first.get('src'):
                    return first['src']
                if isinstance(first, str):
                    return first

        # Fallback to HTML scraping
        time.sleep(self.delay_between_requests)
        response = self.make_request(product_url)
        if not response:
            return ""
        soup = BeautifulSoup(response.text, 'html.parser')

        # Try multiple image extraction strategies for Moida
        img_selectors = [
            'img[src*=".jpg"]',
            'img[src*=".jpeg"]',
            'img[src*=".png"]',
            'img[src*=".webp"]',
            'img[data-src*=".jpg"]',
            'img[data-src*=".png"]',
            'img[data-lazy*=".jpg"]',
            'img[data-lazy*=".png"]',
            'img[class*="product"]',
            'img[class*="main"]',
            'img[alt*="product"]',
            'img[src*="cdn.shopify.com"]',
            'img[src*="moidaus.com"]',
            'img'
        ]
        
        for selector in img_selectors:
            img_elements = soup.select(selector)
            for img_elem in img_elements:
                img_url = img_elem.get('src') or img_elem.get('data-src') or img_elem.get('data-lazy')
                if img_url and len(img_url) > 10:
                    if not img_url.startswith('http'):
                        img_url = urljoin(self.base_url, img_url)
                    if 'moidaus.com' in img_url.lower() or 'cdn.shopify.com' in img_url.lower():
                        logger.info(f"Found image: {img_url}")
                        return img_url
        
        # If no image found, return empty string
        logger.warning(f"No image found for {product_name}")
        return ""
    
    def scrape_individual_product_page(self, product_url: str) -> Dict:
        """Scrape individual product page for detailed information (brand, price, ingredients, images)"""
        if not product_url:
            return {}
        
        logger.info(f"Scraping individual product page: {product_url}")
        
        # Add delay before scraping individual page
        time.sleep(self.delay_between_requests)
        
        response = self.make_request(product_url)
        if not response:
            return {}
        soup = BeautifulSoup(response.text, 'html.parser')
        self._remove_noise_tags(soup)
        
        additional_info: Dict[str, any] = {}
        
        # Prefer structured data from Shopify product JSON
        product_json = self.fetch_product_json(product_url)
        if product_json:
            # Brand/vendor
            vendor = product_json.get('vendor') or ''
            if vendor:
                additional_info['vendor'] = vendor
            # Title/name
            title_from_json = product_json.get('title') or ''
            if title_from_json:
                additional_info['name'] = title_from_json.strip()
            # Images
            images = product_json.get('images') or []
            image_urls: List[str] = []
            for img in images:
                if isinstance(img, dict) and img.get('src'):
                    image_urls.append(img['src'])
                elif isinstance(img, str):
                    image_urls.append(img)
            if image_urls:
                additional_info['main_image'] = image_urls[0]
                additional_info['image_url'] = image_urls[0]
                additional_info['additional_images'] = image_urls[1:]
            # Price from variants (use minimum variant price)
            try:
                variants = product_json.get('variants') or []
                prices: List[Decimal] = []
                for v in variants:
                    norm = self.normalize_price(v.get('price'))
                    if norm:
                        try:
                            prices.append(Decimal(norm.replace('$', '')))
                        except InvalidOperation:
                            continue
                if prices:
                    additional_info['price'] = f"${min(prices):.2f}"
            except Exception:
                pass
            # Ingredients from body_html if present
            body_html = product_json.get('body_html') or ''
            ing_from_body = self.extract_ingredients_from_body_html(body_html)
            if ing_from_body:
                additional_info['ingredients'] = ing_from_body
        
        try:
            # Extract price from product page only if not already set from JSON
            if not additional_info.get('price'):
                # Try JSON-LD first
                jsonld_price = self.extract_price_from_json_ld(soup)
                if jsonld_price:
                    additional_info['price'] = jsonld_price
                
                price_selectors = [
                    '[class*="sale"]',
                    '[class*="current-price"]',
                    '[class*="product-price"]',
                    '[class*="price"]',
                ]
                for selector in price_selectors:
                    price_elem = soup.select_one(selector)
                    if price_elem:
                        price_text = price_elem.get_text().strip()
                        # Prefer the smallest $ value found to avoid compare-at
                        matches = re.findall(r'\$\s*(\d+[\.,]?\d*)', price_text)
                        if matches:
                            values: List[Decimal] = []
                            for m in matches:
                                try:
                                    values.append(Decimal(m.replace(',', '')))
                                except InvalidOperation:
                                    continue
                            if values:
                                additional_info['price'] = f"${min(values):.2f}"
                                break
            
            # Extract detailed description
            desc_selectors = [
                '[class*="description"]',
                '[class*="product-description"]',
                '[class*="details"]',
                'p[class*="description"]',
                '[class*="product-details"]'
            ]
            
            for selector in desc_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem and desc_elem.get_text().strip():
                    additional_info['detailed_description'] = desc_elem.get_text().strip()
                    break
            
            # Extract ingredients: look for heading/label 'Ingredients' then capture content
            ingredients_text = ''
            # 1) Direct ingredients containers by class/id
            ingredients_selectors = [
                '[class*="ingredients"]',
                '[id*="ingredients"]',
                '[data-tab*="ingredients"]',
                '[data-accordion*="ingredients"]',
                'div[class*="ingredients"]',
                'section[class*="ingredients"]'
            ]
            for selector in ingredients_selectors:
                elem = soup.select_one(selector)
                if elem and elem.get_text(strip=True):
                    ingredients_text = self._sanitize_text(elem.get_text(separator=' ', strip=True))
                    break
            # 2) If not found, find a node whose text is 'Ingredients' and read next sibling/content
            if not ingredients_text:
                # More robust extraction using helper
                ingredients_text = self.extract_ingredients_from_page(soup)
            if ingredients_text:
                additional_info['ingredients'] = ingredients_text
            
            # Extract vendor/brand
            vendor_selectors = [
                '[class*="vendor"]',
                '[class*="brand"]',
                'span:contains("Vendor:")',
                'div:contains("Vendor:")'
            ]
            
            for selector in vendor_selectors:
                vendor_elem = soup.select_one(selector)
                if vendor_elem and vendor_elem.get_text().strip():
                    vendor_text = vendor_elem.get_text().strip()
                    if 'Vendor:' in vendor_text:
                        vendor_name = vendor_text.replace('Vendor:', '').strip()
                        additional_info['vendor'] = vendor_name
                    break
            
            # Derive brand from title if possible
            page_title = additional_info.get('name', '')
            if not page_title:
                title_el = soup.find('h1') or soup.find(['h2', 'h3'], class_=re.compile(r'title|product', re.I))
                page_title = title_el.get_text(strip=True) if title_el else ''
            derived_brand = self.derive_brand_from_title(page_title)
            if derived_brand:
                additional_info['brand'] = derived_brand
            elif 'vendor' in additional_info and additional_info['vendor']:
                additional_info['brand'] = additional_info['vendor']
            
        except Exception as e:
            logger.error(f"Error scraping individual product page: {e}")
        
        return additional_info
    
    def backfill_output(self, filename: str = None, limit: Optional[int] = None) -> bool:
        """Re-scrape existing entries to populate missing fields like price and ingredients."""
        if filename is None:
            filename = self.output_file
        if not os.path.exists(filename):
            logger.warning(f"Output file not found for backfill: {filename}")
            return False
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            products: List[Dict] = data.get('products', [])
            total = len(products) if limit is None else min(limit, len(products))
            logger.info(f"Starting backfill for {total} products from {filename}")
            updated: List[Dict] = []
            for idx, prod in enumerate(products[:total]):
                url = prod.get('product_url') or ''
                if not url:
                    updated.append(prod)
                    continue
                try:
                    # Scrape fresh details
                    add = self.scrape_individual_product_page(url)
                    merged = {**prod, **add}
                    # Ensure name and brand
                    if not merged.get('name') and add.get('name'):
                        merged['name'] = add['name']
                    if not merged.get('brand'):
                        merged['brand'] = self.derive_brand_from_title(merged.get('name', '')) or merged.get('vendor', '')
                    # Normalize price
                    if merged.get('price'):
                        merged['price'] = self.normalize_price(merged['price'])
                    # Ensure image_url present if main_image exists
                    if not merged.get('image_url') and merged.get('main_image'):
                        merged['image_url'] = merged['main_image']
                    updated.append(merged)
                    logger.info(f"Backfilled {idx+1}/{total}: {merged.get('name','')} ")
                    time.sleep(self.delay_between_requests)
                except Exception as e:
                    logger.warning(f"Backfill failed for {url}: {e}")
                    updated.append(prod)
            # Preserve remainder if limit used
            if total < len(products):
                updated.extend(products[total:])
            data['products'] = updated
            data.setdefault('scraper_info', {})['scraped_at'] = datetime.now().isoformat()
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Backfill complete. Updated {len(updated)} products")
            return True
        except Exception as e:
            logger.error(f"Backfill error: {e}")
            return False
    
    def scrape_batched(self) -> List[Dict]:
        """Batched scraping with progress tracking"""
        logger.info("Starting batched Moida scraping...")
        
        # Step 1: Load collection URLs and discover products from each
        collection_urls = self.load_collection_urls()
        self.last_source_urls = collection_urls[:]
        all_products: List[Dict] = []
        for coll_url in collection_urls:
            discovered = self.discover_products_from_collection_page(coll_url)
            if not discovered:
                continue
            all_products.extend(discovered)
            # Respect global cap early
            if len(all_products) >= self.max_total_products:
                break
        if not all_products:
            logger.warning("No products found from provided collections")
            return []
        
        # Step 2: Take only the first batch_size products per run, bounded by global max
        cap = min(self.batch_size, self.max_total_products)
        batch_products = all_products[:cap]
        logger.info(f"Selected {len(batch_products)} products for scraping")
        
        # Step 3: Scrape individual product pages
        scraped_products = []
        for i, product in enumerate(batch_products):
            logger.info(f"Processing product {i+1}/{len(batch_products)}: {product['name']}")
            
            # Create base product info
            product_info = {
                'name': product['name'],
                'product_url': self.canonicalize_product_url(product['url']),
                'category': product['category'],
                'brand': product['brand'],
                'vendor': product.get('vendor', ''),
                'scraped_at': datetime.now().isoformat(),
                'image_url': product.get('image_url', ''),
                'price': product.get('price', ''),
                'detailed_description': '',
                'ingredients': '',
                'main_image': '',
                'additional_images': []
            }
            
            # Extract image from individual product page if not already found
            if not product_info['image_url']:
                image_url = self.extract_image_from_product_page(product_info['product_url'], product['name'])
                if image_url:
                    product_info['image_url'] = image_url
                    product_info['main_image'] = image_url
            
            # Get additional info from individual page
            additional_info = self.scrape_individual_product_page(product_info['product_url'])
            product_info.update(additional_info)
            # Ensure brand present using name heuristic if still missing
            if not product_info.get('brand'):
                product_info['brand'] = self.derive_brand_from_title(product_info.get('name', ''))
            # Ensure image_url mirrors main_image if needed
            if not product_info.get('image_url') and product_info.get('main_image'):
                product_info['image_url'] = product_info['main_image']
            
            # Mark as scraped
            # Store canonical relative path to prevent duplicates
            canonical_absolute = self.canonicalize_product_url(product['url'])
            canonical_path = urlparse(canonical_absolute).path
            self.scraped_urls.add(canonical_absolute)
            self.scraped_urls.add(canonical_path)
            
            # Only add if we have an image URL
            if product_info['image_url']:
                scraped_products.append(product_info)
                logger.info(f"Added product with image: {product['name']} - {product_info['image_url']}")
            else:
                logger.warning(f"Skipping product without image: {product['name']}")
            
            # Add delay between products
            time.sleep(self.delay_between_requests)
        
        # Save progress
        self.save_progress()
        
        logger.info(f"Total products scraped with images: {len(scraped_products)}")
        return scraped_products
    
    def save_to_json(self, products: List[Dict], filename: str = None):
        """Save scraped products to JSON file"""
        if filename is None:
            filename = self.output_file
            
        try:
            # Filter products to ensure they have image URLs
            products_with_images = [p for p in products if p.get('image_url')]
            
            # Load existing products if file exists
            existing_products = []
            if os.path.exists(filename):
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        existing_products = existing_data.get('products', [])
                        logger.info(f"Loaded {len(existing_products)} existing products")
                except Exception as e:
                    logger.error(f"Error loading existing data: {e}")
            
            # Combine existing and new products with de-duplication by product_url
            combined_by_url: Dict[str, Dict] = {}
            for p in existing_products:
                url = p.get('product_url') or ''
                if url:
                    combined_by_url[url] = p
            for p in products_with_images:
                url = p.get('product_url') or ''
                if url:
                    combined_by_url[url] = p  # prefer latest scrape
            all_products = list(combined_by_url.values())
            
            output_data = {
                'scraper_info': {
                    'scraped_at': datetime.now().isoformat(),
                    'source_urls': self.last_source_urls,
                    'total_products': len(all_products),
                    'scraper_version': '1.0.0',
                    'enhanced_features': [
                        'Batched scraping (10 products per category)',
                        'Maximum 60 products per run',
                        'Progress tracking to avoid duplicates',
                        'Individual product page scraping',
                        'Mandatory image URLs for all products',
                        'Real data only',
                        'Ethical rate limiting',
                        'Comprehensive product information',
                        'Shopify-based website support'
                    ],
                    'scraping_stats': {
                        'total_scraped_this_run': len(products),
                        'products_with_images_this_run': len(products_with_images),
                        'total_products_all_runs': len(all_products),
                        'previously_scraped_urls': len(self.scraped_urls)
                    }
                },
                'products': all_products
            }
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully saved {len(products_with_images)} new products to {filename}")
            logger.info(f"Total products in file: {len(all_products)}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return False
    
    def run(self):
        """Main method to run the scraper"""
        logger.info("Starting Batched Moida scraper...")
        
        try:
            # Scrape products using batched approach
            products = self.scrape_batched()
            
            if products:
                # Save to JSON
                success = self.save_to_json(products)
                if success:
                    logger.info("Batched scraping completed successfully!")
                    # After saving, run a quick backfill pass on existing file to populate missing fields
                    try:
                        self.backfill_output(self.output_file, limit=50)
                    except Exception:
                        pass
                    return products
                else:
                    logger.error("Failed to save data to JSON")
                    return []
            else:
                logger.warning("No new products found to scrape")
                return []
                
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            return []

def main():
    """Main function to run the scraper"""
    scraper = BatchedMoidaScraper()
    products = scraper.run()
    
    if products:
        print(f"\nSuccessfully scraped {len(products)} new products!")
        print("Data saved to output_moida_batched.json")
        
        # Print first few products as preview
        print("\nSample products from this run:")
        for i, product in enumerate(products[:5]):
            price = product.get('price', 'N/A')
            image = "YES" if product.get('image_url') else "NO"
            print(f"{i+1}. {product.get('name', 'N/A')} - {price} {image}")
        
        # Show statistics
        products_with_prices = sum(1 for p in products if p.get('price'))
        products_with_urls = sum(1 for p in products if p.get('product_url'))
        products_with_images = sum(1 for p in products if p.get('image_url'))
        print(f"\nStatistics for this run:")
        print(f"   - Products with prices: {products_with_prices}/{len(products)}")
        print(f"   - Products with URLs: {products_with_urls}/{len(products)}")
        print(f"   - Products with images: {products_with_images}/{len(products)}")
        print(f"   - Total products scraped this run: {len(products)}")
        print(f"   - Previously scraped URLs: {len(scraper.scraped_urls)}")
    else:
        print("No new products were scraped")

if __name__ == "__main__":
    main() 