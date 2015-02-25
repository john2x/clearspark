class Jigsaw:
    def _jigsaw_login(self, company_name):
        browser = Browser('phantomjs')
        browser.visit('https://connect.data.com/login')
        browser.find_by_css('#j_username').first.fill('robin@customerohq.com')
        browser.find_by_css('#j_password').first.fill('951562nileppeZ')
        browser.find_by_css('#login_btn').first.click()
        time.sleep(1)
        return browser

    def _company_search(self, browser):
        browser.visit("http://connect.data.com/search#p=advancedsearch;;t=companies;;ss=tabchanged")
        browser.find_by_css("#findCompaniesTab").first.click()
        browser.find_by_name("companies")[1].fill("guidespark")
        time.sleep(1)
        browser.find_by_css('.search-button')[-1].click()
        browser.find_by_css('.companyName')
        return browser
