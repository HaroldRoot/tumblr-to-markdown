import logging
import re
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
import xmltodict

import settings

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class DownloadWorker:
    def __init__(self, proxies: dict[str, str] = None) -> None:
        self.proxies = proxies

    def download_posts(self, name: str, num: int, start: int, target_dir: Path) -> None:
        """Downloads blog posts from Tumblr API."""
        url = f"https://{name}.tumblr.com/api/read?num={num}&start={start}"
        logger.info(f"Fetching URL: {url}")
        try:
            response = requests.get(url=url, proxies=self.proxies, timeout=settings.TIMEOUT)
            response.raise_for_status()
            self.process_response(response.content, target_dir)
        except requests.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}")

    def process_response(self, content: bytes, target_dir: Path) -> None:
        """Processes API response and save posts."""
        try:
            data = xmltodict.parse(content.decode('utf-8'))
            posts = data.get("tumblr", {}).get("posts", {}).get("post", [])
            if not posts:
                logger.warning("No posts found.")
                return

            if isinstance(posts, dict):  # Handle case with a single post
                posts = [posts]

            for post in posts:
                logger.info(f"Processing Post: {post.get('@id', 'Unknown ID')}")
                self.save_post(post, target_dir)
        except (KeyError, UnicodeDecodeError) as e:
            logger.error(f"Error processing response: {e}")

    def download_image(self, url: str, target_dir: Path) -> None:
        target_dir.mkdir(parents=True, exist_ok=True)
        response = requests.get(url=url, proxies=self.proxies, timeout=settings.TIMEOUT)

        if response.status_code == 200:
            image_name = url.split('/')[-1]
            image_path = target_dir / image_name

            with open(image_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded: {image_name}")
        else:
            logger.error(f"Failed to download image from {url}")

    def regular_post_to_markdown(self, post: dict, target_dir: Path) -> str:
        title = f"# {post.get('regular-title', '')}\n\n"

        body = post.get('regular-body', '')
        body = self.handle_images(body, target_dir)

        tags = post.get('tag', '').split()
        tag_str = ' '.join(f'#{tag}' for tag in tags)
        tag_str = '\n' + tag_str

        markdown = title + body + "\n" + tag_str
        return markdown

    def photo_post_to_markdown(self, post: dict, target_dir: Path) -> str:
        caption = post.get('photo-caption', '')

        attachment_dir = target_dir / 'attachments'
        attachment_dir.mkdir(parents=True, exist_ok=True)

        photoset = post.get('photoset', None)

        photos = ""

        if photoset:
            for photo in photoset.get("photo"):
                max_width_url = photo["photo-url"][0]["#text"]
                file_name = self.download_photo(max_width_url, attachment_dir)
                photos += f'![[{file_name}]]\n\n'
        else:
            max_width_url = post["photo-url"][0]["#text"]
            file_name = self.download_photo(max_width_url, attachment_dir)
            photos += f'![[{file_name}]]\n\n'

        tags = post.get('tag', '').split()
        tag_str = ' '.join(f'#{tag}' for tag in tags)
        tag_str = '\n' + tag_str

        markdown = photos + caption + "\n" + tag_str
        return markdown

    @staticmethod
    def download_photo(url: str, target_dir: Path) -> str:
        if not url:
            return ""

        file_name = url.split("/")[-1]
        file_path = target_dir / file_name

        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(file_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded: {file_name}")
        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")

        return file_name

    @staticmethod
    def chat_post_to_markdown(post: dict) -> str:
        title = f"# {post.get('conversation-title', '')}\n\n"

        body = post.get('conversation-text', '')

        tags = post.get('tag', '').split()
        tag_str = ' '.join(f'#{tag}' for tag in tags)
        tag_str = '\n' + tag_str

        markdown = "\n" + title + body + "\n" + tag_str
        return markdown

    def save_post(self, post: dict, target_dir: Path) -> None:
        """Saves a single post to a file."""
        date_gmt = post.get("@date-gmt", "").split(" ")[0]
        slug = post.get("@slug", "")
        filename = f"{date_gmt}-{slug}.md" if slug else f"{date_gmt}.md"
        file_path = target_dir / filename

        table_fields = ['@url-with-slug', '@type', '@date-gmt', '@date']
        table_rows = ("| key | value |\n"
                      "| --- | ----- |\n")

        for field in table_fields:
            key = field.replace('@', '')
            value = post.get(field, '')
            table_rows += f"| {key:<13} | {value} |\n"

        md_content = table_rows + "\n"
        post_type = post.get("@type", "")

        match post_type:
            case "Regular":
                md_content += self.regular_post_to_markdown(post=post, target_dir=target_dir)
            case "Photo":
                md_content += self.photo_post_to_markdown(post=post, target_dir=target_dir)
            case "Conversation":
                md_content += self.chat_post_to_markdown(post=post)
            case _:
                logger.error(f"Unknown post type: {post_type}")

        try:
            with file_path.open("w", encoding="utf-8") as f:
                f.write(md_content)
            logger.info(f"Saved post to {file_path}")
        except IOError as e:
            logger.error(f"Failed to save post to {file_path}: {e}")

    def handle_images(self, body: str, target_dir: Path) -> str:
        img_paragraphs = self.extract_img_paragraphs(body)
        updated_paragraphs = self.move_imgs_to_end(img_paragraphs)
        updated_paragraphs = self.replace_img_with_markdown(updated_paragraphs, target_dir)
        return self.update_body(body, img_paragraphs, updated_paragraphs)

    @staticmethod
    def extract_img_paragraphs(body):
        img_paragraphs = re.findall(r'<p>.*?<img.*?</p>', body, re.DOTALL)
        return img_paragraphs

    @staticmethod
    def move_imgs_to_end(paragraphs):
        updated_paragraphs = []
        img_pattern = re.compile(r'<img.*?>')

        for paragraph in paragraphs:
            imgs = img_pattern.findall(paragraph)
            paragraph_without_imgs = img_pattern.sub('', paragraph).strip()

            if re.sub(r'<[^>]*>', '', paragraph_without_imgs).strip() == '':
                updated_paragraph = '\n\n' + '\n'.join(imgs)
            else:
                updated_paragraph = paragraph_without_imgs + '\n\n' + '\n'.join(imgs)

            updated_paragraphs.append(updated_paragraph)

        return updated_paragraphs

    def replace_img_with_markdown(self, paragraphs, target_dir: Path):
        updated_paragraphs = []
        img_pattern = re.compile(r'<img.*?src="([^"]+/([^/]+?))".*?>')

        attachments_dir = target_dir / 'attachments'
        attachments_dir.mkdir(parents=True, exist_ok=True)

        for paragraph in paragraphs:
            def download_and_replace(match):
                img_url = match.group(1)
                img_filename = match.group(2)
                self.download_image(img_url, attachments_dir)
                return f'![[{img_filename}]]'

            new_paragraph = img_pattern.sub(download_and_replace, paragraph)
            updated_paragraphs.append(new_paragraph)

        return updated_paragraphs

    @staticmethod
    def update_body(body, old_paragraphs, new_paragraphs):
        for old, new in zip(old_paragraphs, new_paragraphs):
            body = body.replace(old, new)
        return body


class CrawlerScheduler(object):
    def __init__(self, names: list[str], proxies: dict[str, str] = None) -> None:
        self.names = names
        self.proxies = proxies
        self.worker = DownloadWorker(proxies=self.proxies)

    def schedule_tasks(self) -> None:
        """Schedules download tasks for each blog."""
        with ThreadPoolExecutor(max_workers=settings.THREADS) as executor:
            futures = []
            for name in self.names:
                total = self.get_total_post_count(name)
                if total > 0:
                    futures.extend(self.schedule_blog_download(executor, name, total))

            for future in futures:
                future.result()
            logger.info("Completed downloading all posts.")

    def schedule_blog_download(self, executor: ThreadPoolExecutor, name: str, total: int) -> list:
        """Schedules the download tasks for a specific blog."""
        target_dir = Path("results") / name
        target_dir.mkdir(parents=True, exist_ok=True)

        num = settings.API_READ_NUM
        futures = []
        for start in range(settings.API_READ_START, total, num):
            futures.append(executor.submit(self.worker.download_posts, name, num, start, target_dir))
        return futures

    def get_total_post_count(self, name: str) -> int:
        """Retrieves the total number of posts for a blog."""
        url = f"https://{name}.tumblr.com/api/read"
        try:
            response = requests.get(url=url, proxies=self.proxies, timeout=settings.TIMEOUT)
            response.raise_for_status()
            if response.status_code == 404:
                logger.warning(f"{name} doesn't exist.")
                return 0
            data = xmltodict.parse(response.content.decode('utf-8'))
            total_posts = data["tumblr"]["posts"]["@total"]
            logger.info(f"{name} has {total_posts} posts.")
            return int(total_posts)
        except (requests.RequestException, KeyError, UnicodeDecodeError) as e:
            logger.error(f"Failed to retrieve post count for {name}: {e}")
            return 0


def load_config() -> dict:
    config_file = Path(__file__).resolve().parent / "config.toml"

    try:
        with config_file.open("rb") as f:
            config = tomllib.load(f)
            logger.info(f"Loaded configuration: {config}")
            return config
    except (IOError, tomllib.TOMLDecodeError) as e:
        logger.error(f"Error reading config file {config_file}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    config = load_config()
    names = config.get("names", [])
    proxies = config.get("proxies", {})

    if not names:
        logger.error('''Please write the site names in the config.toml file.
For example:
names = ["name1", "name2"]''')
        sys.exit(1)

    scheduler = CrawlerScheduler(names=names, proxies=proxies)
    scheduler.schedule_tasks()
