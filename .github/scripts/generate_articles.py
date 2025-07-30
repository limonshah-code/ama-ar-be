# import os
# import time
# import random
# from dotenv import load_dotenv
# from paapi5_python_sdk.api.default_api import DefaultApi
# from paapi5_python_sdk.models.search_items_request import SearchItemsRequest
# from paapi5_python_sdk.models.search_items_resource import SearchItemsResource
# from paapi5_python_sdk.models.get_items_resource import GetItemsResource
# from paapi5_python_sdk.models.get_items_request import GetItemsRequest
# from paapi5_python_sdk.models.partner_type import PartnerType
# from paapi5_python_sdk.rest import ApiException
# import base64
# import json
# import requests
# import markdown
# import mimetypes
# import yaml
# import re
# import smtplib
# import csv
# from email.mime.multipart import MIMEMultipart
# from email.mime.base import MIMEBase
# from email.mime.text import MIMEText
# from email import encoders
# from datetime import datetime, timedelta
# from PIL import Image
# from google import genai
# from google.genai import types
# import numpy as np
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.metrics.pairwise import cosine_similarity
# import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize

# # Configure output directory
# OUTPUT_DIR = "generated-articles"
# KEYWORDS_FILE = "data/keywords.txt"
# PROCESSED_KEYWORDS_FILE = "data/processed_keywords.txt"
# GENERATED_KEYWORDS_FILE = "data/keywords-generated.txt"
# LINKS_FILE = "data/links.txt"
# ARTICLES_PER_RUN = 2
# TOP_LINKS_COUNT = 20

# # Ensure output directories exist
# os.makedirs(OUTPUT_DIR, exist_ok=True)
# os.makedirs(os.path.dirname(PROCESSED_KEYWORDS_FILE), exist_ok=True)
# os.makedirs(os.path.dirname(GENERATED_KEYWORDS_FILE), exist_ok=True)
# os.makedirs(os.path.dirname(LINKS_FILE), exist_ok=True)

# # Download NLTK data if needed
# try:
#     nltk.data.find('tokenizers/punkt')
#     nltk.data.find('corpora/stopwords')
# except LookupError:
#     nltk.download('punkt')
#     nltk.download('stopwords')

# # Load Amazon credentials from .env
# load_dotenv()
# access_key = os.getenv("AMAZON_ACCESS_KEY")
# secret_key = os.getenv("AMAZON_SECRET_KEY")
# partner_tag = os.getenv("AMAZON_PARTNER_TAG")

# # PAAPI host and region
# host = "webservices.amazon.com"
# region = "us-east-1"

# class APIKeyManager:
#     """Manages multiple Gemini API keys with rotation and quota tracking"""
    
#     def __init__(self):
#         self.api_keys = self._load_api_keys()
#         self.current_key_index = 0
#         self.key_usage_count = {}
#         self.max_requests_per_key = 60
#         self.failed_keys = set()
        
#         for key in self.api_keys:
#             self.key_usage_count[key] = 0
    
#     def _load_api_keys(self):
#         keys = []
#         for i in range(1, 7):
#             key = os.environ.get(f"GEMINI_API_KEY_{i}")
#             if key:
#                 keys.append(key)
#                 print(f"Loaded API key #{i}")
#         original_key = os.environ.get("GEMINI_API_KEY")
#         if original_key and original_key not in keys:
#             keys.append(original_key)
#             print("Loaded original GEMINI_API_KEY")
#         if not keys:
#             raise ValueError("No Gemini API keys found in environment variables")
#         print(f"Total API keys loaded: {len(keys)}")
#         return keys
    
#     def get_current_key(self):
#         if not self.api_keys:
#             raise ValueError("No API keys available")
#         current_key = self.api_keys[self.current_key_index]
#         if (current_key in self.failed_keys or
#             self.key_usage_count[current_key] >= self.max_requests_per_key):
#             self._rotate_key()
#             current_key = self.api_keys[self.current_key_index]
#         return current_key
    
#     def _rotate_key(self):
#         attempts = 0
#         while attempts < len(self.api_keys):
#             self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
#             current_key = self.api_keys[self.current_key_index]
#             if (current_key not in self.failed_keys and
#                 self.key_usage_count[current_key] < self.max_requests_per_key):
#                 print(f"Rotated to API key #{self.current_key_index + 1}")
#                 return
#             attempts += 1
#         raise Exception("All API keys have been exhausted or failed")
    
#     def increment_usage(self, key):
#         if key in self.key_usage_count:
#             self.key_usage_count[key] += 1
#             print(f"API key usage: {self.key_usage_count[key]}/{self.max_requests_per_key}")
    
#     def mark_key_as_failed(self, key, error_message):
#         self.failed_keys.add(key)
#         print(f"Marked API key as failed: {error_message}")
#         if key == self.api_keys[self.current_key_index]:
#             try:
#                 self._rotate_key()
#             except Exception as e:
#                 print(f"Failed to rotate key: {e}")
    
#     def get_status(self):
#         status = {}
#         for i, key in enumerate(self.api_keys):
#             key_id = f"Key_{i+1}"
#             status[key_id] = {
#                 "usage": self.key_usage_count[key],
#                 "max_requests": self.max_requests_per_key,
#                 "failed": key in self.failed_keys,
#                 "active": i == self.current_key_index
#             }
#         return status

# # Initialize API key manager
# api_key_manager = APIKeyManager()

# def print_separator(title="", length=80):
#     if title:
#         padding = (length - len(title) - 2) // 2
#         print("=" * padding + f" {title} " + "=" * padding)
#     else:
#         print("=" * length)

# def safe_get_value(obj, *keys):
#     current = obj
#     for key in keys:
#         if hasattr(current, key):
#             current = getattr(current, key)
#         elif isinstance(current, dict) and key in current:
#             current = current[key]
#         else:
#             return "N/A"
#     if hasattr(current, 'display_value'):
#         return current.display_value
#     elif hasattr(current, 'display_values') and current.display_values:
#         return current.display_values
#     elif current is not None:
#         return str(current)
#     else:
#         return "N/A"

# def has_data(obj, *keys):
#     current = obj
#     for key in keys:
#         if hasattr(current, key):
#             current = getattr(current, key)
#         elif isinstance(current, dict) and key in current:
#             current = current[key]
#         else:
#             return False
#     if hasattr(current, 'display_value'):
#         return current.display_value and current.display_value != "N/A"
#     elif hasattr(current, 'display_values'):
#         return current.display_values and len(current.display_values) > 0
#     elif current is not None:
#         return str(current) != "N/A" and str(current).strip() != ""
#     else:
#         return False

# def rate_limited_delay(min_seconds=2, max_seconds=3):
#     delay = random.uniform(min_seconds, max_seconds)
#     print(f"   ⏳ Waiting {delay:.1f} seconds to avoid rate limiting...")
#     time.sleep(delay)

# def fetch_amazon_products(keyword, min_results=7, max_results=12):
#     """Fetch Amazon product data and return as a structured list."""
#     try:
#         default_api = DefaultApi(
#             access_key=access_key,
#             secret_key=secret_key,
#             host=host,
#             region=region
#         )
#         search_resources = [
#             SearchItemsResource.ITEMINFO_TITLE,
#             SearchItemsResource.ITEMINFO_BYLINEINFO,
#             SearchItemsResource.ITEMINFO_CLASSIFICATIONS,
#             SearchItemsResource.ITEMINFO_CONTENTINFO,
#             SearchItemsResource.ITEMINFO_CONTENTRATING,
#             SearchItemsResource.ITEMINFO_EXTERNALIDS,
#             SearchItemsResource.ITEMINFO_FEATURES,
#             SearchItemsResource.ITEMINFO_MANUFACTUREINFO,
#             SearchItemsResource.ITEMINFO_PRODUCTINFO,
#             SearchItemsResource.ITEMINFO_TECHNICALINFO,
#             SearchItemsResource.ITEMINFO_TRADEININFO,
#             SearchItemsResource.IMAGES_PRIMARY_SMALL,
#             SearchItemsResource.IMAGES_PRIMARY_MEDIUM,
#             SearchItemsResource.IMAGES_PRIMARY_LARGE
#         ]
#         target_results = random.randint(min_results, max_results)
#         request = SearchItemsRequest(
#             partner_tag=partner_tag,
#             partner_type=PartnerType.ASSOCIATES,
#             keywords=keyword,
#             resources=search_resources,
#             marketplace="www.amazon.com",
#             search_index="All",
#             item_count=target_results
#         )
#         print_separator(f"AMAZON PRODUCT SEARCH: '{keyword.upper()}'")
#         print(f"🎯 Fetching {target_results} products...\n")
#         response = default_api.search_items(request)
#         products = []
#         if response.search_result is not None and response.search_result.items:
#             items = response.search_result.items
#             print(f"✅ Found {len(items)} products")
#             for idx, item in enumerate(items, start=1):
#                 product_data = {
#                     "asin": item.asin,
#                     "title": safe_get_value(item, 'item_info', 'title'),
#                     "url": item.detail_page_url,
#                     "brand": safe_get_value(item, 'item_info', 'by_line_info', 'brand'),
#                     "features": safe_get_value(item, 'item_info', 'features') if has_data(item, 'item_info', 'features') else [],
#                     "image_large": safe_get_value(item, 'images', 'primary', 'large', 'url'),
#                     "image_medium": safe_get_value(item, 'images', 'primary', 'medium', 'url'),
#                     "image_small": safe_get_value(item, 'images', 'primary', 'small', 'url')
#                 }
#                 products.append(product_data)
#                 rate_limited_delay()
#         else:
#             print("❌ No items found for the keyword:", keyword)
#         return products
#     except ApiException as e:
#         print(f"❌ API Exception: {e}")
#         return []
#     except Exception as e:
#         print(f"❌ Unexpected error: {e}")
#         return []

# def save_binary_file(file_name, data):
#     with open(file_name, "wb") as f:
#         f.write(data)

# def compress_image(image_path, quality=65):
#     try:
#         with Image.open(image_path) as img:
#             webp_path = f"{os.path.splitext(image_path)[0]}.webp"
#             img.save(webp_path, 'WEBP', quality=quality)
#             os.remove(image_path)
#             return webp_path
#     except Exception as e:
#         print(f"Image compression error: {e}")
#         return image_path

# def upload_to_cloudinary(file_path, resource_type="image"):
#     url = f"https://api.cloudinary.com/v1_1/{os.environ['CLOUDINARY_CLOUD_NAME']}/{resource_type}/upload"
#     payload = {
#         'upload_preset': 'ml_default',
#         'api_key': os.environ['CLOUDINARY_API_KEY']
#     }
#     try:
#         with open(file_path, 'rb') as f:
#             files = {'file': f}
#             response = requests.post(url, data=payload, files=files)
#         if response.status_code == 200:
#             return response.json()['secure_url']
#         print(f"Upload failed: {response.text}")
#         return None
#     except Exception as e:
#         print(f"Upload error: {e}")
#         return None

# def generate_and_upload_image(title):
#     max_retries = 3
#     for attempt in range(max_retries):
#         try:
#             current_key = api_key_manager.get_current_key()
#             client = genai.Client(api_key=current_key)
#             model = "gemini-2.0-flash-exp-image-generation"
#             contents = [types.Content(
#                 role="user",
#                 parts=[types.Part.from_text(text=f"""Create a realistic blog header image for the topic: "{title}". Include the title text "{title}" overlaid on the image as a stylish, readable heading. Use clean typography. Image size should be 16:9 aspect ratio.""")]
#             )]
#             response = client.models.generate_content(
#                 model=model,
#                 contents=contents,
#                 config=types.GenerateContentConfig(response_modalities=["image", "text"])
#             )
#             api_key_manager.increment_usage(current_key)
#             if not response.candidates or not response.candidates[0].content.parts:
#                 print(f"No valid image data in response (attempt {attempt + 1})")
#                 if attempt < max_retries - 1:
#                     time.sleep(2)
#                     continue
#                 return None
#             inline_data = response.candidates[0].content.parts[0].inline_data
#             if not hasattr(inline_data, 'mime_type') or not inline_data.mime_type:
#                 print(f"Invalid mime_type in response (attempt {attempt + 1})")
#                 if attempt < max_retries - 1:
#                     time.sleep(2)
#                     continue
#                 return None
#             file_ext = mimetypes.guess_extension(inline_data.mime_type)
#             if not file_ext:
#                 print(f"Could not determine file extension (attempt {attempt + 1})")
#                 if attempt < max_retries - 1:
#                     time.sleep(2)
#                     continue
#                 return None
#             original_file = f"blog_image_{int(time.time())}{file_ext}"
#             save_binary_file(original_file, inline_data.data)
#             final_file = compress_image(original_file)
#             image_url = upload_to_cloudinary(final_file)
#             if image_url:
#                 return image_url
#             print(f"Image upload failed (attempt {attempt + 1})")
#             if attempt < max_retries - 1:
#                 time.sleep(2)
#                 continue
#             return None
#         except Exception as e:
#             error_str = str(e).lower()
#             if any(keyword in error_str for keyword in ['quota', 'resource_exhausted', 'limit']):
#                 print(f"Quota exhausted for current API key (attempt {attempt + 1}): {e}")
#                 api_key_manager.mark_key_as_failed(current_key, str(e))
#                 if attempt < max_retries - 1:
#                     print("Retrying with next API key...")
#                     time.sleep(2)
#                     continue
#             else:
#                 print(f"Image generation error (attempt {attempt + 1}): {e}")
#                 if attempt < max_retries - 1:
#                     time.sleep(2)
#                     continue
#             if attempt == max_retries - 1:
#                 print("Max retries reached for image generation")
#                 return None

# def create_slug(title):
#     slug = title.lower()
#     slug = re.sub(r'[^a-z0-9\s-]', '', slug)
#     slug = re.sub(r'[\s-]+', '-', slug)
#     slug = slug.strip('-')
#     slug = slug[:100]
#     return slug

# def link_to_keywords(link):
#     url_path = link.split('beacleaner.com/')[-1].strip('/')
#     keywords = url_path.replace('-', ' ')
#     return keywords

# def find_relevant_links(target_keyword, links, top_n=TOP_LINKS_COUNT):
#     if not links:
#         return []
#     link_keywords = [link_to_keywords(link) for link in links]
#     all_texts = link_keywords + [target_keyword]
#     vectorizer = TfidfVectorizer(stop_words='english')
#     tfidf_matrix = vectorizer.fit_transform(all_texts)
#     target_vector = tfidf_matrix[-1]
#     link_vectors = tfidf_matrix[:-1]
#     similarities = cosine_similarity(target_vector, link_vectors).flatten()
#     link_sim_pairs = list(zip(links, similarities))
#     link_sim_pairs.sort(key=lambda x: x[1], reverse=True)
#     top_links = [pair[0] for pair in link_sim_pairs[:top_n]]
#     print(f"Found {len(top_links)} relevant links for keyword: {target_keyword}")
#     return top_links

# def create_article_prompt(title, article_number, image_url, products):
#     tomorrow = datetime.now() + timedelta(days=1)
#     publish_date = tomorrow.strftime("%Y-%m-%d")
#     slug = create_slug(title)
#     canonical_url = f"https://www.beacleaner.com/{slug}"
#     existing_links = get_existing_links()
#     relevant_links = find_relevant_links(title, existing_links)
#     links_json = json.dumps(relevant_links, indent=2)
#     products_json = json.dumps(products, indent=2)
#     prompt = f"""Based on the title: "{title}", create a comprehensive, SEO-optimized Amazon product roundup article:

# This will be article #{article_number} in the series.

# ---

# publishDate: {publish_date}T00:00:00Z
# title: {title}
# excerpt: Discover the best {title.lower()} options on Amazon with detailed reviews and features for 7-12 top products.
# image: {image_url}
# category: Product Reviews
# tags:
#   - {title.lower()}
#   - amazon products
#   - best buys
# metadata:
#   canonical: {canonical_url}

# ---

# Article Structure Requirements:

# 1. **Title (H2):** Include primary keyword "{title}" near beginning, under 60 characters
# 2. **Introduction (150-200 words):** Hook the reader, use "{title}" in first 100 words, outline the article
# 3. **Takeaway:** Bullet points summarizing key benefits of top products
# 4. **Quick Answer:** 40-60 words answering "What are the best {title.lower()} on Amazon?"
# 5. **Main Body:** 7-12 H2 sections (one per product from the data below):
#    - Use product title as H2 heading
#    - 200-300 words per section
#    - Include product features as bullet points
#    - Embed Amazon affiliate link: [Buy on Amazon](<product.url>) using the 'url' field from each product in the data
#    - Use product image if available from 'image_medium' or 'image_large' fields
#    - Include "{title}" and related keywords
#    - Use 1-2 relevant internal links from: {links_json}
# 6. **FAQ Section:** 4-6 questions about "{title}", 50-75 words each
# 7. **Conclusion (150-200 words):** Summarize top picks, use "{title}", include CTA to check products

# Amazon Product Data:
# {products_json}

# Ensure the article:
# - Uses natural keyword inclusion
# - High readability, conversational tone
# - 2,500–3,000 words
# - Proper Markdown format with H2/H3 tags
# - Includes unique insights
# - Balances NLP-friendly clarity (60%) with engaging content (40%)

# Provide the complete article in Markdown format.
# """
#     return prompt

# def generate_article(prompt):
#     max_retries = 3
#     for attempt in range(max_retries):
#         try:
#             current_key = api_key_manager.get_current_key()
#             client = genai.Client(api_key=current_key)
#             model = "gemini-2.5-flash-preview-05-20"
#             contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
#             generate_content_config = types.GenerateContentConfig(
#                 temperature=1,
#                 top_p=0.95,
#                 top_k=64,
#                 max_output_tokens=8192,
#                 response_mime_type="text/plain",
#             )
#             print(f"Generating article (attempt {attempt + 1})...")
#             full_response = ""
#             for chunk in client.models.generate_content_stream(
#                 model=model,
#                 contents=contents,
#                 config=generate_content_config,
#             ):
#                 if chunk.text:
#                     print(chunk.text, end="", flush=True)
#                     full_response += chunk.text
#             api_key_manager.increment_usage(current_key)
#             print("\nArticle generation complete.")
#             return full_response if full_response else "No content generated"
#         except Exception as e:
#             error_str = str(e).lower()
#             if any(keyword in error_str for keyword in ['quota', 'resource_exhausted', 'limit']):
#                 print(f"Quota exhausted for current API key (attempt {attempt + 1}): {e}")
#                 api_key_manager.mark_key_as_failed(current_key, str(e))
#                 if attempt < max_retries - 1:
#                     print("Retrying with next API key...")
#                     time.sleep(2)
#                     continue
#             else:
#                 print(f"Error generating article (attempt {attempt + 1}): {e}")
#                 if attempt < max_retries - 1:
#                     time.sleep(2)
#                     continue
#             if attempt == max_retries - 1:
#                 return f"Error generating article after {max_retries} attempts: {e}"

# def send_email_notification(titles, article_urls, recipient_email="homecosycreation@gmail.com"):
#     from_email = "limon.working@gmail.com"
#     app_password = os.environ.get("EMAIL_PASSWORD")
#     if not app_password:
#         print("Email password not set. Skipping email notification.")
#         return False
#     msg = MIMEMultipart()
#     msg['From'] = from_email
#     msg['To'] = recipient_email
#     msg['Subject'] = f"Generated Amazon Roundup Articles - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
#     api_status = api_key_manager.get_status()
#     status_text = "\n\nAPI Key Usage Status:\n"
#     for key_id, status in api_status.items():
#         status_text += f"{key_id}: {status['usage']}/{status['max_requests']} requests"
#         if status['failed']:
#             status_text += " (FAILED)"
#         if status['active']:
#             status_text += " (ACTIVE)"
#         status_text += "\n"
#     body = f"The following Amazon product roundup articles have been generated:\n\n"
#     for i, (title, url) in enumerate(zip(titles, article_urls), 1):
#         body += f"{i}. {title}\n   URL: {url}\n\n"
#     body += status_text
#     msg.attach(MIMEText(body, 'plain'))
#     try:
#         server = smtplib.SMTP('smtp.gmail.com', 587)
#         server.starttls()
#         server.login(from_email, app_password)
#         server.send_message(msg)
#         server.quit()
#         print(f"Email notification sent successfully to {recipient_email}")
#         return True
#     except Exception as e:
#         print(f"Failed to send email notification: {e}")
#         return False

# def read_keywords_from_csv(filename=KEYWORDS_FILE):
#     try:
#         keywords = []
#         if os.path.exists(filename):
#             with open(filename, 'r', encoding='utf-8') as f:
#                 reader = csv.reader(f)
#                 for row in reader:
#                     if row and row[0].strip():
#                         keywords.append(row[0].strip())
#         else:
#             print(f"Keywords file {filename} not found. Creating new file.")
#             os.makedirs(os.path.dirname(filename), exist_ok=True)
#             with open(filename, 'w', encoding='utf-8'):
#                 pass
#         return keywords
#     except Exception as e:
#         print(f"Error reading keywords from CSV: {e}")
#         return []

# def write_keywords_to_csv(keywords, filename=KEYWORDS_FILE):
#     try:
#         os.makedirs(os.path.dirname(filename), exist_ok=True)
#         with open(filename, 'w', encoding='utf-8', newline='') as f:
#             writer = csv.writer(f)
#             for keyword in keywords:
#                 writer.writerow([keyword])
#         return True
#     except Exception as e:
#         print(f"Error writing keywords to CSV: {e}")
#         return False

# def append_processed_keywords(keywords, urls, filename=PROCESSED_KEYWORDS_FILE):
#     try:
#         os.makedirs(os.path.dirname(filename), exist_ok=True)
#         file_exists = os.path.exists(filename) and os.path.getsize(filename) > 0
#         with open(filename, 'a', encoding='utf-8') as f:
#             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             if not file_exists:
#                 f.write("# Processed Keywords Log\n")
#                 f.write("# Format: [TIMESTAMP] KEYWORD - URL\n\n")
#             f.write(f"## Batch processed on {timestamp}\n")
#             for keyword, url in zip(keywords, urls):
#                 f.write(f"{url}\n")
#             f.write("\n")
#         return True
#     except Exception as e:
#         print(f"Error appending to processed keywords file: {e}")
#         return False

# def append_to_generated_keywords(keywords, filename=GENERATED_KEYWORDS_FILE):
#     try:
#         os.makedirs(os.path.dirname(filename), exist_ok=True)
#         with open(filename, 'a', encoding='utf-8') as f:
#             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             for keyword in keywords:
#                 f.write(f"{keyword}\n")
#         return True
#     except Exception as e:
#         print(f"Error appending to generated keywords file: {e}")
#         return False

# def append_to_links_file(old_urls, new_urls, filename=LINKS_FILE):
#     try:
#         os.makedirs(os.path.dirname(filename), exist_ok=True)
#         existing_links = set()
#         if os.path.exists(filename) and os.path.getsize(filename) > 0:
#             with open(filename, 'r', encoding='utf-8') as f:
#                 existing_links = {line.strip() for line in f if line.strip()}
#         all_urls = old_urls + new_urls
#         unique_new_urls = [url for url in all_urls if url not in existing_links]
#         if unique_new_urls:
#             with open(filename, 'a', encoding='utf-8') as f:
#                 timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 for url in unique_new_urls:
#                     f.write(f"{url}\n")
#                 f.write("\n")
#             return True
#         else:
#             print("No new unique URLs to append")
#             return True
#     except Exception as e:
#         print(f"Error appending to links file: {e}")
#         return False

# def get_existing_links(filename=LINKS_FILE):
#     existing_links = []
#     if os.path.exists(filename):
#         with open(filename, 'r', encoding='utf-8') as f:
#             for line in f:
#                 line = line.strip()
#                 if line and not line.startswith('#') and 'https://' in line:
#                     existing_links.append(line)
#     return existing_links

# def get_keywords(filename=KEYWORDS_FILE, count=ARTICLES_PER_RUN):
#     try:
#         all_keywords = read_keywords_from_csv(filename)
#         if not all_keywords:
#             print("No keywords found in the CSV file or file is empty.")
#             return [" "], []
#         if len(all_keywords) <= count:
#             selected_keywords = all_keywords.copy()
#             return selected_keywords, selected_keywords
#         last_index = 0
#         track_file = ".last_keyword_index"
#         if os.path.exists(track_file):
#             with open(track_file, 'r') as f:
#                 try:
#                     last_index = int(f.read().strip())
#                 except ValueError:
#                     last_index = 0
#         start_index = last_index % len(all_keywords)
#         end_index = start_index + count
#         if end_index <= len(all_keywords):
#             selected_keywords = all_keywords[start_index:end_index]
#         else:
#             selected_keywords = all_keywords[start_index:] + all_keywords[:end_index - len(all_keywords)]
#         with open(track_file, 'w') as f:
#             f.write('0')
#         return selected_keywords, selected_keywords
#     except Exception as e:
#         print(f"Error reading keywords file: {e}")
#         return [" "], []

# def update_keyword_files(all_used_keywords, article_urls):
#     if not all_used_keywords:
#         print("No keywords to update.")
#         return
#     all_keywords = read_keywords_from_csv(KEYWORDS_FILE)
#     remaining_keywords = [k for k in all_keywords if k not in all_used_keywords]
#     if write_keywords_to_csv(remaining_keywords, KEYWORDS_FILE):
#         print(f"Removed {len(all_used_keywords)} used keywords from Text file.")
#     else:
#         print("Failed to update CSV file with remaining keywords.")
#     if append_processed_keywords(all_used_keywords, article_urls, PROCESSED_KEYWORDS_FILE):
#         print(f"Added {len(all_used_keywords)} keywords to processed keywords file with URLs.")
#     else:
#         print("Failed to update processed keywords file.")
#     if append_to_generated_keywords(all_used_keywords, GENERATED_KEYWORDS_FILE):
#         print(f"Added {len(all_used_keywords)} keywords to generated keywords file.")
#     else:
#         print("Failed to update generated keywords file.")
#     existing_links = get_existing_links(LINKS_FILE)
#     old_links = ["https://www.beacleaner.com/how-to-clean-a-ceiling"]
#     filtered_old_links = [link for link in old_links if link not in existing_links]
#     filtered_new_links = [link for link in article_urls if link not in existing_links]
#     if filtered_old_links or filtered_new_links:
#         if append_to_links_file(filtered_old_links, filtered_new_links, LINKS_FILE):
#             print(f"Added {len(filtered_old_links)} old links and {len(filtered_new_links)} new links to links file.")
#         else:
#             print("Failed to update links file.")

# def main():
#     print(f"Starting article generation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"API Key Manager Status:")
#     for key_id, status in api_key_manager.get_status().items():
#         print(f"  {key_id}: {'Active' if status['active'] else 'Standby'}")
#     keywords, keywords_to_track = get_keywords()
#     print(f"Selected {len(keywords)} keywords for processing")
#     generated_files = []
#     successful_keywords = []
#     article_urls = []
#     default_image_url = "https://res.cloudinary.com/dbcpfy04c/image/upload/v1743184673/images_k6zam3.png"
#     for i, title in enumerate(keywords, 1):
#         print(f"\n{'='*50}\nGenerating Article #{i}: {title}\n{'='*50}")
#         try:
#             slug = create_slug(title)
#             article_url = f"https://beacleaner.com/{slug}"
#             image_url = generate_and_upload_image(title)
#             if not image_url:
#                 print(f"Using default image URL: {default_image_url}")
#                 image_url = default_image_url
#             else:
#                 print(f"Generated image URL: {image_url}")
#             products = fetch_amazon_products(title)
#             if not products:
#                 print("No products fetched, skipping to next keyword")
#                 continue
#             prompt = create_article_prompt(title, i, image_url, products)
#             article = generate_article(prompt)
#             if article.startswith("Error"):
#                 print("Article generation failed, skipping to next keyword")
#                 continue
#             filename = f"{OUTPUT_DIR}/{slug}.md"
#             with open(filename, "w", encoding="utf-8") as f:
#                 f.write(article)
#             generated_files.append(filename)
#             successful_keywords.append(title)
#             article_urls.append(article_url)
#             print(f"Article saved to {filename}")
#             print(f"Article URL: {article_url}")
#         except Exception as e:
#             print(f"Error processing keyword '{title}': {e}")
#             continue
#         if i < len(keywords):
#             print("Waiting 10 seconds before next article...")
#             time.sleep(10)
#     print(f"\nFinal API Key Usage Status:")
#     for key_id, status in api_key_manager.get_status().items():
#         print(f"  {key_id}: {status['usage']}/{status['max_requests']} requests"
#               f"{' (FAILED)' if status['failed'] else ''}")
#     update_keyword_files(successful_keywords, article_urls)
#     if generated_files:
#         send_email_notification(successful_keywords, article_urls)
#     print(f"Article generation completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"Generated {len(generated_files)} articles")
#     print(f"Successfully processed {len(successful_keywords)} keywords")
#     for i, (keyword, url) in enumerate(zip(successful_keywords, article_urls), 1):
#         print(f"{i}. {keyword} → {url}")

# if __name__ == "__main__":
#     main()






import os
import time
import random
from dotenv import load_dotenv
from paapi5_python_sdk.api.default_api import DefaultApi
from paapi5_python_sdk.models.search_items_request import SearchItemsRequest
from paapi5_python_sdk.models.search_items_resource import SearchItemsResource
from paapi5_python_sdk.models.get_items_resource import GetItemsResource
from paapi5_python_sdk.models.get_items_request import GetItemsRequest
from paapi5_python_sdk.models.partner_type import PartnerType
from paapi5_python_sdk.rest import ApiException
import base64
import json
import requests
import markdown
import mimetypes
import yaml
import re
import smtplib
import csv
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime, timedelta
from PIL import Image
from google import genai
from google.genai import types
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Configure output directory
OUTPUT_DIR = "generated-articles"
KEYWORDS_FILE = "data/keywords.txt"
PROCESSED_KEYWORDS_FILE = "data/processed_keywords.txt"
GENERATED_KEYWORDS_FILE = "data/keywords-generated.txt"
LINKS_FILE = "data/links.txt"
ARTICLES_PER_RUN = 2
TOP_LINKS_COUNT = 20

# Ensure output directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(PROCESSED_KEYWORDS_FILE), exist_ok=True)
os.makedirs(os.path.dirname(GENERATED_KEYWORDS_FILE), exist_ok=True)
os.makedirs(os.path.dirname(LINKS_FILE), exist_ok=True)

# Download NLTK data if needed
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

# Load Amazon credentials from .env
load_dotenv()
access_key = os.getenv("AMAZON_ACCESS_KEY")
secret_key = os.getenv("AMAZON_SECRET_KEY")
partner_tag = os.getenv("AMAZON_PARTNER_TAG")

# PAAPI host and region
host = "webservices.amazon.com"
region = "us-east-1"

class APIKeyManager:
    """Manages multiple Gemini API keys with rotation and quota tracking"""
    
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.key_usage_count = {}
        self.max_requests_per_key = 60
        self.failed_keys = set()
        
        for key in self.api_keys:
            self.key_usage_count[key] = 0
    
    def _load_api_keys(self):
        keys = []
        for i in range(1, 7):
            key = os.environ.get(f"GEMINI_API_KEY_{i}")
            if key:
                keys.append(key)
                print(f"Loaded API key #{i}")
        original_key = os.environ.get("GEMINI_API_KEY")
        if original_key and original_key not in keys:
            keys.append(original_key)
            print("Loaded original GEMINI_API_KEY")
        if not keys:
            raise ValueError("No Gemini API keys found in environment variables")
        print(f"Total API keys loaded: {len(keys)}")
        return keys
    
    def get_current_key(self):
        if not self.api_keys:
            raise ValueError("No API keys available")
        current_key = self.api_keys[self.current_key_index]
        if (current_key in self.failed_keys or
            self.key_usage_count[current_key] >= self.max_requests_per_key):
            self._rotate_key()
            current_key = self.api_keys[self.current_key_index]
        return current_key
    
    def _rotate_key(self):
        attempts = 0
        while attempts < len(self.api_keys):
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            current_key = self.api_keys[self.current_key_index]
            if (current_key not in self.failed_keys and
                self.key_usage_count[current_key] < self.max_requests_per_key):
                print(f"Rotated to API key #{self.current_key_index + 1}")
                return
            attempts += 1
        raise Exception("All API keys have been exhausted or failed")
    
    def increment_usage(self, key):
        if key in self.key_usage_count:
            self.key_usage_count[key] += 1
            print(f"API key usage: {self.key_usage_count[key]}/{self.max_requests_per_key}")
    
    def mark_key_as_failed(self, key, error_message):
        self.failed_keys.add(key)
        print(f"Marked API key as failed: {error_message}")
        if key == self.api_keys[self.current_key_index]:
            try:
                self._rotate_key()
            except Exception as e:
                print(f"Failed to rotate key: {e}")
    
    def get_status(self):
        status = {}
        for i, key in enumerate(self.api_keys):
            key_id = f"Key_{i+1}"
            status[key_id] = {
                "usage": self.key_usage_count[key],
                "max_requests": self.max_requests_per_key,
                "failed": key in self.failed_keys,
                "active": i == self.current_key_index
            }
        return status

# Initialize API key manager
api_key_manager = APIKeyManager()

def print_separator(title="", length=80):
    if title:
        padding = (length - len(title) - 2) // 2
        print("=" * padding + f" {title} " + "=" * padding)
    else:
        print("=" * length)

def safe_get_value(obj, *keys):
    current = obj
    for key in keys:
        if hasattr(current, key):
            current = getattr(current, key)
        elif isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return "N/A"
    if hasattr(current, 'display_value'):
        return current.display_value
    elif hasattr(current, 'display_values') and current.display_values:
        return current.display_values
    elif current is not None:
        return str(current)
    else:
        return "N/A"

def has_data(obj, *keys):
    current = obj
    for key in keys:
        if hasattr(current, key):
            current = getattr(current, key)
        elif isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return False
    if hasattr(current, 'display_value'):
        return current.display_value and current.display_value != "N/A"
    elif hasattr(current, 'display_values'):
        return current.display_values and len(current.display_values) > 0
    elif current is not None:
        return str(current) != "N/A" and str(current).strip() != ""
    else:
        return False

def rate_limited_delay(min_seconds=2, max_seconds=3):
    delay = random.uniform(min_seconds, max_seconds)
    print(f"   ⏳ Waiting {delay:.1f} seconds to avoid rate limiting...")
    time.sleep(delay)

def fetch_amazon_products(keyword, min_results=7, max_results=12):
    """Fetch Amazon product data and return as a structured list."""
    try:
        default_api = DefaultApi(
            access_key=access_key,
            secret_key=secret_key,
            host=host,
            region=region
        )
        search_resources = [
            SearchItemsResource.ITEMINFO_TITLE,
            SearchItemsResource.ITEMINFO_BYLINEINFO,
            SearchItemsResource.ITEMINFO_CLASSIFICATIONS,
            SearchItemsResource.ITEMINFO_CONTENTINFO,
            SearchItemsResource.ITEMINFO_CONTENTRATING,
            SearchItemsResource.ITEMINFO_EXTERNALIDS,
            SearchItemsResource.ITEMINFO_FEATURES,
            SearchItemsResource.ITEMINFO_MANUFACTUREINFO,
            SearchItemsResource.ITEMINFO_PRODUCTINFO,
            SearchItemsResource.ITEMINFO_TECHNICALINFO,
            SearchItemsResource.ITEMINFO_TRADEININFO,
            SearchItemsResource.IMAGES_PRIMARY_SMALL,
            SearchItemsResource.IMAGES_PRIMARY_MEDIUM,
            SearchItemsResource.IMAGES_PRIMARY_LARGE
        ]
        target_results = random.randint(min_results, max_results)
        request = SearchItemsRequest(
            partner_tag=partner_tag,
            partner_type=PartnerType.ASSOCIATES,
            keywords=keyword,
            resources=search_resources,
            marketplace="www.amazon.com",
            search_index="All",
            item_count=target_results
        )
        print_separator(f"AMAZON PRODUCT SEARCH: '{keyword.upper()}'")
        print(f"🎯 Fetching {target_results} products...\n")
        response = default_api.search_items(request)
        products = []
        if response.search_result is not None and response.search_result.items:
            items = response.search_result.items
            print(f"✅ Found {len(items)} products")
            for idx, item in enumerate(items, start=1):
                product_data = {
                    "asin": item.asin,
                    "title": safe_get_value(item, 'item_info', 'title'),
                    "url": item.detail_page_url,
                    "brand": safe_get_value(item, 'item_info', 'by_line_info', 'brand'),
                    "features": safe_get_value(item, 'item_info', 'features') if has_data(item, 'item_info', 'features') else [],
                    "image_large": safe_get_value(item, 'images', 'primary', 'large', 'url'),
                    "image_medium": safe_get_value(item, 'images', 'primary', 'medium', 'url'),
                    "image_small": safe_get_value(item, 'images', 'primary', 'small', 'url')
                }
                products.append(product_data)
                rate_limited_delay()
        else:
            print("❌ No items found for the keyword:", keyword)
        return products
    except ApiException as e:
        print(f"❌ API Exception: {e}")
        return []
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return []

def save_binary_file(file_name, data):
    with open(file_name, "wb") as f:
        f.write(data)

def compress_image(image_path, quality=65):
    try:
        with Image.open(image_path) as img:
            webp_path = f"{os.path.splitext(image_path)[0]}.webp"
            img.save(webp_path, 'WEBP', quality=quality)
            os.remove(image_path)
            return webp_path
    except Exception as e:
        print(f"Image compression error: {e}")
        return image_path

def upload_to_cloudinary(file_path, resource_type="image"):
    url = f"https://api.cloudinary.com/v1_1/{os.environ['CLOUDINARY_CLOUD_NAME']}/{resource_type}/upload"
    payload = {
        'upload_preset': 'ml_default',
        'api_key': os.environ['CLOUDINARY_API_KEY']
    }
    try:
        with open(file_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(url, data=payload, files=files)
        if response.status_code == 200:
            return response.json()['secure_url']
        print(f"Upload failed: {response.text}")
        return None
    except Exception as e:
        print(f"Upload error: {e}")
        return None

def generate_and_upload_image(title):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            current_key = api_key_manager.get_current_key()
            client = genai.Client(api_key=current_key)
            model = "gemini-2.0-flash-exp-image-generation"
            contents = [types.Content(
                role="user",
                parts=[types.Part.from_text(text=f"""Create a realistic blog header image for the topic: "{title}". Include the title text "{title}" overlaid on the image as a stylish, readable heading. Use clean typography. Image size should be 16:9 aspect ratio.""")]
            )]
            response = client.models.generate_content(
                model=model,
                contents=contents,
                config=types.GenerateContentConfig(response_modalities=["image", "text"])
            )
            api_key_manager.increment_usage(current_key)
            if not response.candidates or not response.candidates[0].content.parts:
                print(f"No valid image data in response (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            inline_data = response.candidates[0].content.parts[0].inline_data
            if not hasattr(inline_data, 'mime_type') or not inline_data.mime_type:
                print(f"Invalid mime_type in response (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            file_ext = mimetypes.guess_extension(inline_data.mime_type)
            if not file_ext:
                print(f"Could not determine file extension (attempt {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                return None
            original_file = f"blog_image_{int(time.time())}{file_ext}"
            save_binary_file(original_file, inline_data.data)
            final_file = compress_image(original_file)
            image_url = upload_to_cloudinary(final_file)
            if image_url:
                return image_url
            print(f"Image upload failed (attempt {attempt + 1})")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            return None
        except Exception as e:
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['quota', 'resource_exhausted', 'limit']):
                print(f"Quota exhausted for current API key (attempt {attempt + 1}): {e}")
                api_key_manager.mark_key_as_failed(current_key, str(e))
                if attempt < max_retries - 1:
                    print("Retrying with next API key...")
                    time.sleep(2)
                    continue
            else:
                print(f"Image generation error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
            if attempt == max_retries - 1:
                print("Max retries reached for image generation")
                return None

def create_slug(title):
    slug = title.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s-]+', '-', slug)
    slug = slug.strip('-')
    slug = slug[:100]
    return slug

def link_to_keywords(link):
    url_path = link.split('beacleaner.com/')[-1].strip('/')
    keywords = url_path.replace('-', ' ')
    return keywords

def find_relevant_links(target_keyword, links, top_n=TOP_LINKS_COUNT):
    if not links:
        return []
    link_keywords = [link_to_keywords(link) for link in links]
    all_texts = link_keywords + [target_keyword]
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix = vectorizer.fit_transform(all_texts)
    target_vector = tfidf_matrix[-1]
    link_vectors = tfidf_matrix[:-1]
    similarities = cosine_similarity(target_vector, link_vectors).flatten()
    link_sim_pairs = list(zip(links, similarities))
    link_sim_pairs.sort(key=lambda x: x[1], reverse=True)
    top_links = [pair[0] for pair in link_sim_pairs[:top_n]]
    print(f"Found {len(top_links)} relevant links for keyword: {target_keyword}")
    return top_links

def extract_category_from_title(title):
    """Extract product category from title for better organization"""
    title_lower = title.lower()
    
    # Common product category mappings
    category_keywords = {
        'cleaning': ['cleaner', 'cleaning', 'detergent', 'soap', 'sanitizer'],
        'tools': ['tool', 'drill', 'saw', 'hammer', 'wrench', 'screwdriver'],
        'kitchen': ['kitchen', 'cooking', 'cookware', 'utensil', 'appliance'],
        'home': ['home', 'furniture', 'decor', 'storage', 'organization'],
        'electronics': ['electronic', 'device', 'gadget', 'tech', 'smart'],
        'outdoor': ['outdoor', 'garden', 'patio', 'camping', 'hiking'],
        'fitness': ['fitness', 'workout', 'exercise', 'yoga', 'gym']
    }
    
    for category, keywords in category_keywords.items():
        if any(keyword in title_lower for keyword in keywords):
            return category
    
    return 'general'

def create_enhanced_article_prompt(title, article_number, image_url, products):
    """Enhanced article prompt following the comprehensive template structure"""
    tomorrow = datetime.now() + timedelta(days=1)
    publish_date = tomorrow.strftime("%Y-%m-%dT%H:%M:%SZ")
    slug = create_slug(title)
    canonical_url = f"https://www.beacleaner.com/{slug}"
    
    # Extract category and create relevant tags
    category = extract_category_from_title(title)
    base_keyword = title.lower().replace('best ', '').replace('top ', '')
    
    existing_links = get_existing_links()
    relevant_links = find_relevant_links(title, existing_links)
    
    # Create tags based on title analysis
    tags = [
        base_keyword,
        f"{category} products",
        "amazon reviews",
        "buying guide"
    ]
    
    products_json = json.dumps(products, indent=2)
    
    prompt = f"""Create a comprehensive Amazon product review article following this exact structure. Use the provided product data to write detailed, helpful content.

---
publishDate: {publish_date}
title: "Top {len(products)} Best {title} in 2025 - Tested & Recommended"
excerpt: "Interested in owning the best {base_keyword}? Get tips on how to pick one that is right for you."
image: {image_url}
category: "Reviews"
tags:
{chr(10).join(f'  - {tag}' for tag in tags)}
metadata:
  canonical: {canonical_url}
---

## ARTICLE STRUCTURE REQUIREMENTS:

### 1. HEADER SECTION (Already provided above)

### 2. Introduction SECTION (150-200 words)
Write an engaging introduction that:
- Acknowledges the challenge of finding the right {base_keyword}
- Mentions 3-4 key criteria for evaluation (quality, durability, value, performance)
- Introduces the concept of eco-friendly/safe options if applicable
- Promises to help readers make an informed decision
- Use "{title}" naturally in the first 100 words

### 3. TOP PICKS SUMMARY
Create "Our Top Picks" section with Amazon affiliate links:
- **Best Overall: [Product Name from data] ([Buy on Amazon](product_url))**
- **Best Budget: [Different Product Name] ([Buy on Amazon](product_url))**
- **Best Premium: [Third Product Name] ([Buy on Amazon](product_url))**
- **Best for [Specific Use]: ([Buy on Amazon](product_url))**
- **Best for [Specific Use]: ([Buy on Amazon](product_url))**
- **Best for [Specific Use]: ([Buy on Amazon](product_url))**

Each pick needs 2-3 sentences explaining why it earned that designation.


### 4. PRODUCT REVIEWS SECTION
Title: "{len(products)} Best {title}"

For EACH product in the data, create:

#### Product Format:
### [Number]. [Category Rank]: [Full Product Name from data]

**Product Details:**
- Use the actual product title from the data
- Include Amazon affiliate link: [Buy on Amazon](product_url_from_data)
- Write 150-200 words covering:
  - Main benefits and standout features
  - Technical capabilities from product features
  - What's included/accessories
  - Performance expectations
  - Who this product is best suited for
  - Value proposition

**Key Features:**
- Create 4-6 bullet points with ✅ emojis
- Use actual features from the product data when available
- Focus on practical benefits
- Include safety and ease-of-use points

**Considerations:**
- Include 1-2 realistic drawbacks with ⚠️ emojis
- Present as constructive feedback, not dealbreakers

### 5. EDUCATIONAL CONTENT

#### Value Proposition Section
Title: "Is it A Wise Decision to Buy {base_keyword}?"
- Justify the investment (150-200 words)
- Compare to alternatives
- Highlight long-term benefits
- Address cost concerns

#### Best Practices Section
Title: "What Is the Best Way to Choose {base_keyword}?"
- Step-by-step selection guide
- Professional tips
- Common mistakes to avoid
- When to upgrade vs. budget options

### 6. FAQ SECTION
Create 8-10 FAQs covering:
- "What is the best {base_keyword} for beginners?"
- "How much should I spend on {base_keyword}?"
- "What features are most important in {base_keyword}?"
- "How do I maintain my {base_keyword}?"
- "Are expensive {base_keyword} worth it?"
- "What brands make the best {base_keyword}?"
- "How long do {base_keyword} typically last?"
- "Can I find good {base_keyword} on a budget?"

Each answer should be 50-75 words with actionable advice.

### 7. FINAL VERDICT
- Restate top recommendation with key selling points
- Mention budget option for cost-conscious buyers
- Highlight premium option for quality seekers
- End with clear, actionable next steps

## WRITING GUIDELINES:

**Tone & Voice:**
- Conversational and helpful
- Authoritative but approachable
- Use "we tested" and "we recommend" for credibility
- Address reader directly with "you"

**Content Principles:**
- Lead with benefits, support with features
- Use specific details from the product data
- Include safety considerations
- Balance enthusiasm with honest assessment
- Natural keyword integration

**SEO Structure:**
- Use H2/H3 headings properly
- Include internal links where relevant: {json.dumps(relevant_links[:5], indent=2)}
- Write scannable content with bullets and short paragraphs
- Target 2,500-3,000 words total

## AMAZON PRODUCT DATA TO USE:
{products_json}

**CRITICAL INSTRUCTIONS:**
1. Use ACTUAL product titles, features, and URLs from the data above
2. Every product mention must include its Amazon affiliate link
3. Write unique content for each product based on its specific features
4. Maintain consistent quality across all sections
5. Ensure all product claims are based on the provided data
6. Create compelling reasons to buy each product
7. Include proper markdown formatting throughout

Generate the complete article now, following this structure exactly and using the provided product data comprehensively."""

    return prompt

def generate_article(prompt):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            current_key = api_key_manager.get_current_key()
            client = genai.Client(api_key=current_key)
            model = "gemini-1.5-pro-latest"
            contents = [types.Content(role="user", parts=[types.Part.from_text(text=prompt)])]
            generate_content_config = types.GenerateContentConfig(
                temperature=0.8,
                top_p=0.95,
                top_k=64,
                max_output_tokens=32000,
                response_mime_type="text/plain",
            )
            print(f"Generating article (attempt {attempt + 1})...")
            full_response = ""
            for chunk in client.models.generate_content_stream(
                model=model,
                contents=contents,
                config=generate_content_config,
            ):
                if chunk.text:
                    print(chunk.text, end="", flush=True)
                    full_response += chunk.text
            api_key_manager.increment_usage(current_key)
            print("\nArticle generation complete.")
            return full_response if full_response else "No content generated"
        except Exception as e:
            error_str = str(e).lower()
            if any(keyword in error_str for keyword in ['quota', 'resource_exhausted', 'limit']):
                print(f"Quota exhausted for current API key (attempt {attempt + 1}): {e}")
                api_key_manager.mark_key_as_failed(current_key, str(e))
                if attempt < max_retries - 1:
                    print("Retrying with next API key...")
                    time.sleep(2)
                    continue
            else:
                print(f"Error generating article (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
            if attempt == max_retries - 1:
                return f"Error generating article after {max_retries} attempts: {e}"

def send_email_notification(titles, article_urls, recipient_email="homecosycreation@gmail.com"):
    from_email = "limon.working@gmail.com"
    app_password = os.environ.get("EMAIL_PASSWORD")
    if not app_password:
        print("Email password not set. Skipping email notification.")
        return False
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = recipient_email
    msg['Subject'] = f"Generated Amazon Roundup Articles - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    api_status = api_key_manager.get_status()
    status_text = "\n\nAPI Key Usage Status:\n"
    for key_id, status in api_status.items():
        status_text += f"{key_id}: {status['usage']}/{status['max_requests']} requests"
        if status['failed']:
            status_text += " (FAILED)"
        if status['active']:
            status_text += " (ACTIVE)"
        status_text += "\n"
    body = f"The following Amazon product roundup articles have been generated:\n\n"
    for i, (title, url) in enumerate(zip(titles, article_urls), 1):
        body += f"{i}. {title}\n   URL: {url}\n\n"
    body += status_text
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)
        server.quit()
        print(f"Email notification sent successfully to {recipient_email}")
        return True
    except Exception as e:
        print(f"Failed to send email notification: {e}")
        return False

def read_keywords_from_csv(filename=KEYWORDS_FILE):
    try:
        keywords = []
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0].strip():
                        keywords.append(row[0].strip())
        else:
            print(f"Keywords file {filename} not found. Creating new file.")
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w', encoding='utf-8'):
                pass
        return keywords
    except Exception as e:
        print(f"Error reading keywords from CSV: {e}")
        return []

def write_keywords_to_csv(keywords, filename=KEYWORDS_FILE):
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            for keyword in keywords:
                writer.writerow([keyword])
        return True
    except Exception as e:
        print(f"Error writing keywords to CSV: {e}")
        return False

def append_processed_keywords(keywords, urls, filename=PROCESSED_KEYWORDS_FILE):
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        file_exists = os.path.exists(filename) and os.path.getsize(filename) > 0
        with open(filename, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if not file_exists:
                f.write("# Processed Keywords Log\n")
                f.write("# Format: [TIMESTAMP] KEYWORD - URL\n\n")
            f.write(f"## Batch processed on {timestamp}\n")
            for keyword, url in zip(keywords, urls):
                f.write(f"{url}\n")
            f.write("\n")
        return True
    except Exception as e:
        print(f"Error appending to processed keywords file: {e}")
        return False

def append_to_generated_keywords(keywords, filename=GENERATED_KEYWORDS_FILE):
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for keyword in keywords:
                f.write(f"{keyword}\n")
        return True
    except Exception as e:
        print(f"Error appending to generated keywords file: {e}")
        return False

def append_to_links_file(old_urls, new_urls, filename=LINKS_FILE):
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        existing_links = set()
        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_links = {line.strip() for line in f if line.strip()}
        all_urls = old_urls + new_urls
        unique_new_urls = [url for url in all_urls if url not in existing_links]
        if unique_new_urls:
            with open(filename, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                for url in unique_new_urls:
                    f.write(f"{url}\n")
                f.write("\n")
            return True
        else:
            print("No new unique URLs to append")
            return True
    except Exception as e:
        print(f"Error appending to links file: {e}")
        return False

def get_existing_links(filename=LINKS_FILE):
    existing_links = []
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and 'https://' in line:
                    existing_links.append(line)
    return existing_links

def get_keywords(filename=KEYWORDS_FILE, count=ARTICLES_PER_RUN):
    try:
        all_keywords = read_keywords_from_csv(filename)
        if not all_keywords:
            print("No keywords found in the CSV file or file is empty.")
            return [" "], []
        if len(all_keywords) <= count:
            selected_keywords = all_keywords.copy()
            return selected_keywords, selected_keywords
        last_index = 0
        track_file = ".last_keyword_index"
        if os.path.exists(track_file):
            with open(track_file, 'r') as f:
                try:
                    last_index = int(f.read().strip())
                except ValueError:
                    last_index = 0
        start_index = last_index % len(all_keywords)
        end_index = start_index + count
        if end_index <= len(all_keywords):
            selected_keywords = all_keywords[start_index:end_index]
        else:
            selected_keywords = all_keywords[start_index:] + all_keywords[:end_index - len(all_keywords)]
        with open(track_file, 'w') as f:
            f.write('0')
        return selected_keywords, selected_keywords
    except Exception as e:
        print(f"Error reading keywords file: {e}")
        return [" "], []

def update_keyword_files(all_used_keywords, article_urls):
    if not all_used_keywords:
        print("No keywords to update.")
        return
    all_keywords = read_keywords_from_csv(KEYWORDS_FILE)
    remaining_keywords = [k for k in all_keywords if k not in all_used_keywords]
    if write_keywords_to_csv(remaining_keywords, KEYWORDS_FILE):
        print(f"Removed {len(all_used_keywords)} used keywords from Text file.")
    else:
        print("Failed to update CSV file with remaining keywords.")
    if append_processed_keywords(all_used_keywords, article_urls, PROCESSED_KEYWORDS_FILE):
        print(f"Added {len(all_used_keywords)} keywords to processed keywords file with URLs.")
    else:
        print("Failed to update processed keywords file.")
    if append_to_generated_keywords(all_used_keywords, GENERATED_KEYWORDS_FILE):
        print(f"Added {len(all_used_keywords)} keywords to generated keywords file.")
    else:
        print("Failed to update generated keywords file.")
    existing_links = get_existing_links(LINKS_FILE)
    old_links = ["https://www.beacleaner.com/how-to-clean-a-ceiling"]
    filtered_old_links = [link for link in old_links if link not in existing_links]
    filtered_new_links = [link for link in article_urls if link not in existing_links]
    if filtered_old_links or filtered_new_links:
        if append_to_links_file(filtered_old_links, filtered_new_links, LINKS_FILE):
            print(f"Added {len(filtered_old_links)} old links and {len(filtered_new_links)} new links to links file.")
        else:
            print("Failed to update links file.")

def validate_article_quality(article_content, title, products):
    """Validate the generated article meets quality standards"""
    quality_checks = {
        'has_title': title.lower() in article_content.lower(),
        'has_product_links': any(product.get('url', '') in article_content for product in products),
        'sufficient_length': len(article_content) > 2000,
        'has_headings': '##' in article_content,
        'has_bullet_points': '✅' in article_content or '-' in article_content,
        'has_buying_guide': 'buying guide' in article_content.lower() or 'what to look for' in article_content.lower(),
        'has_faq': 'faq' in article_content.lower() or 'frequently asked' in article_content.lower()
    }
    
    passed_checks = sum(quality_checks.values())
    total_checks = len(quality_checks)
    quality_score = (passed_checks / total_checks) * 100
    
    print(f"Article Quality Score: {quality_score:.1f}% ({passed_checks}/{total_checks} checks passed)")
    
    if quality_score < 70:
        print("⚠️ Article quality below threshold. Consider regenerating.")
        for check, passed in quality_checks.items():
            if not passed:
                print(f"   ❌ Failed: {check}")
    
    return quality_score >= 70

def enhance_article_with_internal_links(article_content, relevant_links):
    """Add relevant internal links to the article content"""
    if not relevant_links:
        return article_content
    
    # Common phrases where we can add internal links
    link_opportunities = [
        "cleaning tips",
        "best practices",
        "maintenance guide",
        "how to clean",
        "cleaning methods",
        "product reviews",
        "buying guide"
    ]
    
    enhanced_content = article_content
    links_added = 0
    
    for i, link in enumerate(relevant_links[:3]):  # Limit to 3 internal links
        if links_added >= 3:
            break
            
        # Extract topic from URL for natural linking
        topic = link.split('/')[-1].replace('-', ' ')
        
        # Find a good place to insert the link
        for opportunity in link_opportunities:
            if opportunity in enhanced_content.lower() and link not in enhanced_content:
                # Add the internal link naturally
                enhanced_content = enhanced_content.replace(
                    opportunity,
                    f"[{opportunity}]({link})",
                    1  # Replace only first occurrence
                )
                links_added += 1
                break
    
    if links_added > 0:
        print(f"Added {links_added} internal links to the article")
    
    return enhanced_content

def main():
    print(f"🚀 Starting Enhanced Amazon Article Generation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_separator("INITIALIZATION")
    
    print(f"API Key Manager Status:")
    for key_id, status in api_key_manager.get_status().items():
        print(f"  {key_id}: {'Active' if status['active'] else 'Standby'}")
    
    keywords, keywords_to_track = get_keywords()
    print(f"\n📋 Selected {len(keywords)} keywords for processing:")
    for i, keyword in enumerate(keywords, 1):
        print(f"  {i}. {keyword}")
    
    generated_files = []
    successful_keywords = []
    article_urls = []
    default_image_url = "https://res.cloudinary.com/dbcpfy04c/image/upload/v1743184673/images_k6zam3.png"
    
    for i, title in enumerate(keywords, 1):
        print_separator(f"PROCESSING ARTICLE #{i}: {title.upper()}")
        
        try:
            # Create URL slug and article URL
            slug = create_slug(title)
            article_url = f"https://www.beacleaner.com/{slug}"
            print(f"📝 Article URL: {article_url}")
            
            # Generate and upload header image
            print("\n🎨 Generating header image...")
            image_url = generate_and_upload_image(title)
            if not image_url:
                print(f"⚠️ Using default image URL: {default_image_url}")
                image_url = default_image_url
            else:
                print(f"✅ Generated image URL: {image_url}")
            
            # Fetch Amazon products
            print(f"\n🛒 Fetching Amazon products for: {title}")
            products = fetch_amazon_products(title)
            if not products:
                print("❌ No products fetched, skipping to next keyword")
                continue
            
            print(f"✅ Successfully fetched {len(products)} products")
            
            # Create enhanced article prompt
            print("\n✍️ Creating article prompt...")
            prompt = create_enhanced_article_prompt(title, i, image_url, products)
            
            # Generate article content
            print("\n🤖 Generating article content...")
            article = generate_article(prompt)
            if article.startswith("Error"):
                print("❌ Article generation failed, skipping to next keyword")
                continue
            
            # Validate article quality
            print("\n🔍 Validating article quality...")
            is_quality = validate_article_quality(article, title, products)
            if not is_quality:
                print("⚠️ Article quality is below threshold but proceeding...")
            
            # Enhance with internal links
            print("\n🔗 Adding internal links...")
            existing_links = get_existing_links()
            relevant_links = find_relevant_links(title, existing_links)
            enhanced_article = enhance_article_with_internal_links(article, relevant_links)
            
            # Save article to file
            filename = f"{OUTPUT_DIR}/{slug}.md"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(enhanced_article)
            
            generated_files.append(filename)
            successful_keywords.append(title)
            article_urls.append(article_url)
            
            print(f"✅ Article saved to {filename}")
            print(f"📊 Article stats: {len(enhanced_article)} characters, {len(enhanced_article.split())} words")
            
        except Exception as e:
            print(f"❌ Error processing keyword '{title}': {e}")
            continue
        
        # Rate limiting between articles
        if i < len(keywords):
            print("\n⏳ Waiting 10 seconds before next article...")
            time.sleep(10)
    
    # Final reporting
    print_separator("FINAL RESULTS")
    
    print(f"📈 Final API Key Usage Status:")
    for key_id, status in api_key_manager.get_status().items():
        print(f"  {key_id}: {status['usage']}/{status['max_requests']} requests"
              f"{' (FAILED)' if status['failed'] else ''}")
    
    # Update keyword tracking files
    print("\n📁 Updating keyword files...")
    update_keyword_files(successful_keywords, article_urls)
    
    # Send email notification
    if generated_files:
        print("\n📧 Sending email notification...")
        send_email_notification(successful_keywords, article_urls)
    
    # Final summary
    print_separator("GENERATION COMPLETE")
    print(f"⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📝 Generated: {len(generated_files)} articles")
    print(f"✅ Successfully processed: {len(successful_keywords)} keywords")
    print(f"📊 Success rate: {(len(successful_keywords)/len(keywords)*100):.1f}%")
    
    print(f"\n📋 Generated Articles:")
    for i, (keyword, url) in enumerate(zip(successful_keywords, article_urls), 1):
        print(f"  {i}. {keyword}")
        print(f"     → {url}")
    
    if len(successful_keywords) < len(keywords):
        failed_keywords = [k for k in keywords if k not in successful_keywords]
        print(f"\n⚠️ Failed Keywords ({len(failed_keywords)}):")
        for keyword in failed_keywords:
            print(f"  - {keyword}")

if __name__ == "__main__":
    main()