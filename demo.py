from US_Visa.logger import logging
from US_Visa.exception import USvisaException
#logging.info("welcome all")

try:
    a = 2/0

except Exception as e:
    raise USvisaException(e , sys)