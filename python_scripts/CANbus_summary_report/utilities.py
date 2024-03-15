from datetime import datetime,timedelta



# Function to get the name of the previous month
def get_previous_month():
    today = datetime.today()
    previous_month = today - timedelta(days=today.day)
    return previous_month.strftime('raw_%Y_%m')

def get_current_month():
    today = datetime.today()
    return today.strftime('raw_%Y_%m')


def get_one_day():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")

def get_previous_day():
    yesterday=datetime.today() -timedelta(days=1)
    yesterday_start_time=yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    return yesterday_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")


