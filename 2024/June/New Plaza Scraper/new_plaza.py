import time
import json

from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException


class PlazaAutomation:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.page_load_strategy = 'normal'
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.urls = self.get_urls()

    def login(self):
        credentials = self.get_credentials()
        username = credentials.get('username', '')
        password = credentials.get('password', '')

        self.driver.get('https://plaza.newnewnew.space/')
        self.driver.maximize_window()
        selecto = '.login-plugin-container'

        if not self.is_element_exist(css_selector=selecto, timeout=5):
            return

        login_menu = self.driver.find_element(by=By.CLASS_NAME, value='login-plugin-container')

        login_menu.click()

        username_field = self.driver.find_element(by=By.ID, value='username')
        username_field.send_keys(username)
        time.sleep(1)

        pass_word = '#password'

        if not self.is_element_exist(css_selector=pass_word, timeout=5):
            return

        password_field = self.driver.find_element(by=By.ID, value='password')
        password_field.send_keys(password)
        time.sleep(1)

        self.driver.find_element(by=By.CSS_SELECTOR, value='[value="Inloggen"]').click()

        self.allow_cookies()
        time.sleep(3)

    def navigate_to_urls(self):
        cout = 0
        while True:
            for url in self.urls:
                self.driver.get(url.strip())
                time.sleep(3)
                self.allow_cookies()
                self.check_responses()

            cout += 1
            print(f'Filters are compeleted {cout} time')

    def allow_cookies(self):
        try:
            allow_cookies_button = self.driver.find_element(by=By.ID,
                                                            value='CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll')
            allow_cookies_button.click()

        except NoSuchElementException:
            pass

    def check_responses(self):
        time.sleep(5)
        divs = self.driver.find_elements(by=By.CLASS_NAME, value='list-item-content')

        list_item = '.list-item-content'

        if not self.is_element_exist(css_selector=list_item, timeout=10):
            return

        for div in divs:
            self.driver.execute_script("window.scrollBy(0, 75);")

            if "Je hebt gereageerd" not in div.text:
                url = div.find_element(by=By.CSS_SELECTOR, value='a').get_attribute('href')
                self.open_new_tab(url)

    def open_new_tab(self, url):
        self.driver.execute_script(f"window.open('{url}', '_blank');")
        self.driver.switch_to.window(self.driver.window_handles[-1])
        button = '.reageer-button'

        if not self.is_element_exist(css_selector=button, timeout=10):
            return

        reageer_button = self.driver.find_element(by=By.CSS_SELECTOR, value='.reageer-button')
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        self.driver.execute_script("window.scrollBy(0, -225);")
        reageer_button.click()
        time.sleep(1)
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])

    def close(self):
        self.driver.quit()

    def get_credentials(self):
        with open('credentials.json', 'r') as json_file:
            return json.load(json_file)

    def get_urls(self):
        with open('location url.txt', 'r') as textfile:
            urls = []
            for url in textfile:
                urls.append(url.strip())

            return urls

    def is_element_exist(self, css_selector='', xpath='', timeout=30):
        # Check whether the Element with the given CSS selector or XPATH exists on the page or not

        select_by = By.XPATH if xpath else By.CSS_SELECTOR
        selector = css_selector if css_selector else xpath

        try:

            WebDriverWait(self.driver, timeout).until(EC.visibility_of_element_located(
                (select_by, selector)))

            return True

        except NoSuchElementException:
            return False

        except TimeoutException:
            return False


def main():
    bot = PlazaAutomation()
    bot.login()
    bot.navigate_to_urls()
    bot.close()


if __name__ == "__main__":
    main()
